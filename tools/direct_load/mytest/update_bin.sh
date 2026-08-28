if [ -z "$1" ];then
   echo "Usage: sh $0 observer_branch_name"
   exit 1
fi
branch=$1
observer_url="http://11.166.86.153:8877/observer.${branch}-7u"
obproxy_url=" http://11.166.86.153:8877/obproxy.release"
mkdir -p bin lib etc admin

wget $observer_url  -o ./wget.log -O bin/observer && chmod +x bin/observer
wget $obproxy_url -o ./wget.log -O bin/obproxy && chmod +x bin/obproxy

bin/observer -V
