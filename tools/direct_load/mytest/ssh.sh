if [[ -e ~/.ssh/id_rsa.pub ]];then
  cat ~/.ssh/id_rsa.pub | pgm -A `grep  _hosts conf/configure.ini |sed -e 's/.*=//g; s/,/\n/g; s/ //g; s/ $//g' | sort | uniq` "cat - >>.ssh/authorized_keys"
else
  echo "Please run ssh-keygen to generate public key first"
fi
