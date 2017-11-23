package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	gosync "github.com/Redundancy/go-sync"
	"github.com/Redundancy/go-sync/blocksources"
	"github.com/Redundancy/go-sync/chunks"
	"github.com/Redundancy/go-sync/filechecksum"
	"github.com/Redundancy/go-sync/index"
)

// BlockSize : checksumのブロックサイズ
// テストのため小さい値を使う
const BlockSize = 4

var content = bytes.NewReader([]byte("The quick brown fox jumped over the lazy dog"))

func main() {
	go func() {
		s := http.NewServeMux()
		s.HandleFunc("/content", contentHandler)
		s.HandleFunc("/checksum", checksumHandler)
		err := http.ListenAndServe(":8000", s)
		if err != nil {
			log.Fatalf("Cannot listen server: %v", err)
		}
	}()

	contentURL := "http://localhost:8000/content"
	checksumURL := "http://localhost:8000/checksum?blockSize=%d"

	fs, err := GetSummary(checksumURL, BlockSize)
	if err != nil {
		log.Fatalf("Cannot get summary: %v", err)
	}

	input, err := os.OpenFile("local.txt", os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("Cannot open local file for read: %v", err)
	}

	temp, err := ioutil.TempFile("", "gosync-sample-tempfile")
	if err != nil {
		log.Fatalf("Cannot open temp file for write: %v", err)
	}
	defer os.Remove(temp.Name())

	rsync := MakeRSync(input, contentURL, temp, fs)

	err = rsync.Patch()

	if err != nil {
		log.Printf("Cannot patch: %v\n", err)
	}

	err = rsync.Close()

	if err != nil {
		log.Printf("Cannot close rsync: %v\n", err)
	}

	data, _ := ioutil.ReadFile(temp.Name())
	log.Printf("Patched content: \"%v\"\n", string(data))

	// Just for inspection
	remoteReferenceSource := rsync.Source.(*blocksources.BlockSourceBase)
	log.Printf("Downloaded Bytes: %v\n", remoteReferenceSource.ReadBytes())
}

// ファイルダウンロードのハンドラ
func contentHandler(w http.ResponseWriter, req *http.Request) {
	http.ServeFile(w, req, "remote.txt")
}

// チェックサムダウンロードのハンドラ
func checksumHandler(w http.ResponseWriter, req *http.Request) {
	var blockSize uint64 = 1024 * 1024
	blockSize, err := strconv.ParseUint(req.URL.Query().Get("blockSize"), 10, 32)

	remote, err := os.OpenFile("remote.txt", os.O_RDONLY, 0)
	if err != nil {
		http.NotFound(w, req)
		return
	}

	defer remote.Close()

	info, err := remote.Stat()
	if err != nil {
		http.NotFound(w, req)
		return
	}

	b, err := EncodeChecksumIndex(remote, info.Size(), uint(blockSize))
	if err != nil {
		http.NotFound(w, req)
		return
	}

	http.ServeContent(w, req, "", time.Now(), b)
}

// EncodeChecksumIndex : gosyncのChecksumIndexをエンコードする
func EncodeChecksumIndex(content io.Reader, fileSize int64, blockSize uint) (io.ReadSeeker, error) {
	generator := filechecksum.NewFileChecksumGenerator(uint(blockSize))
	weakSize := generator.WeakRollingHash.Size()
	strongSize := generator.GetStrongHash().Size()
	b := bytes.NewBuffer(nil)
	b.Write(int64ToBytes(fileSize))
	b.Write(intToBytes(weakSize))
	b.Write(intToBytes(strongSize))
	_, err := generator.GenerateChecksums(content, b)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(b.Bytes()), nil
}

// DecodeChecksumIndex : EncodeChecksumIndexでエンコードしたものをデコードする
func DecodeChecksumIndex(reader io.Reader) (fileSize int64, idx *index.ChecksumIndex, lookup filechecksum.ChecksumLookup, err error) {
	fBlock := bytes.NewBuffer(nil)
	wBlock := bytes.NewBuffer(nil)
	sBlock := bytes.NewBuffer(nil)

	_, err = io.CopyN(fBlock, reader, 8)
	if err != nil {
		return
	}

	_, err = io.CopyN(wBlock, reader, 4)
	if err != nil {
		return
	}

	_, err = io.CopyN(sBlock, reader, 4)
	if err != nil {
		return
	}

	fileSize = int64(binary.LittleEndian.Uint64(fBlock.Bytes()))
	weakSize := int(binary.LittleEndian.Uint32(wBlock.Bytes()))
	strongSize := int(binary.LittleEndian.Uint32(sBlock.Bytes()))

	readChunks, err := chunks.LoadChecksumsFromReader(reader, weakSize, strongSize)

	if err != nil {
		return
	}

	idx = index.MakeChecksumIndex(readChunks)
	lookup = chunks.StrongChecksumGetter(readChunks)

	return
}

// GetSummary : サーバからチェックサムを取得する
func GetSummary(urlFormat string, BlockSize int64) (gosync.FileSummary, error) {
	res, err := http.DefaultClient.Get(fmt.Sprintf(urlFormat, BlockSize))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	fileSize, referenceFileIndex, checksumLookup, err := DecodeChecksumIndex(res.Body)
	if err != nil {
		return nil, err
	}

	blockCount := fileSize / BlockSize
	if fileSize%BlockSize != 0 {
		blockCount++
	}

	fs := &gosync.BasicSummary{
		ChecksumIndex:  referenceFileIndex,
		ChecksumLookup: checksumLookup,
		BlockCount:     uint(blockCount),
		BlockSize:      uint(BlockSize),
		FileSize:       fileSize,
	}

	return fs, nil
}

// MakeRSync : gosync.RSync作成する
func MakeRSync(local gosync.ReadSeekerAt, remote string, output io.Writer, fs gosync.FileSummary) *gosync.RSync {
	return &gosync.RSync{
		Input:  local,
		Output: output,
		Source: blocksources.NewBlockSourceBase(
			&HttpRequester{
				Url:    remote,
				Client: http.DefaultClient,
			},
			blocksources.MakeFileSizedBlockResolver(
				uint64(fs.GetBlockSize()),
				fs.GetFileSize(),
			),
			&filechecksum.HashVerifier{
				Hash:                md5.New(),
				BlockSize:           fs.GetBlockSize(),
				BlockChecksumGetter: fs,
			},
			1,
			4*MB,
		),
		Summary: fs,
		OnClose: nil,
	}
}

// helpers
func intToBytes(val int) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(val))
	return bs
}

func int64ToBytes(val int64) []byte {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(val))
	return bs
}
