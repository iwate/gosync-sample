[Redundancy/go-sync](https://github.com/Redundancy/go-sync)を使ってrsyncするサンプル

blocksourcesコピーして`HttpRequester`を書き直してる。

理由は、go-syncの`HttpRequester`の`http.Client`がプライベートだったため。

http.DefaultClientじゃないやつ使いたい時もあるし。