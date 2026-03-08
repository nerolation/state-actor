for port in 8545 8645 8745; do
  curl -s http://localhost:$port -X POST \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x0",false],"id":1}' \
    | python3 -c "import sys,json; r=json.load(sys.stdin)['result']; print(f'port $port: stateRoot={r[\"stateRoot\"]}  hash={r[\"hash\"]}')"
done
