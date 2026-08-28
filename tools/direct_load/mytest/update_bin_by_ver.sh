if [ -z "$1" ];then
   echo "Usage: sh $0 observer_version_name"
   exit 1
fi
branch=$1
observer_url="http://11.166.86.153:8877/observer.${branch}-7u"
obproxy_url=" http://11.166.86.153:8877/obproxy.4_0_0_release"
mkdir -p $branch/{bin,lib,etc,admin}

wget $observer_url  -o ./wget.log -O $branch/bin/observer && chmod +x $branch/bin/observer
wget $obproxy_url -o ./wget.log -O $branch/bin/obproxy && chmod +x $branch/bin/obproxy

$branch/bin/observer -V
