if [ -z "${1+x}" ]; then
        echo "usage: <cmd> tag" >&2
        exit 1
fi

tag="${1}"

docker build -t hub.pingcap.net/tiflash/sdb_data:$tag .
docker push hub.pingcap.net/tiflash/sdb_data:$tag

echo "build sdb_data:${tag} succed"