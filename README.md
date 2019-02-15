golang 读取和写入gz格式压缩文件

gz文件压缩/打开方式: gzip/gunzip

## 使用方式: ##
### 1.1 读取压缩格式文件(按块读取) ###
./vdn_gz -i /vdncloud/gowork/src/vdn_gz/b.gz -o /vdncloud/gowork/src/vdn_gz/a.gz -g -b 2048

### 1.2 读取压缩格式文件(按行读取) ###
./vdn_gz -i /vdncloud/gowork/src/vdn_gz/b.gz -o /vdncloud/gowork/src/vdn_gz/a.gz -g -l

### 2.1 读取普通格式文件(按块读取) ###
./vdn_gz -i /vdncloud/gowork/src/vdn_gz/b.gz -o /vdncloud/gowork/src/vdn_gz/a.gz -b 2048

### 2.2 读取普通格式文件(按行读取) ###
./vdn_gz -i /vdncloud/gowork/src/vdn_gz/b.gz -o /vdncloud/gowork/src/vdn_gz/a.gz -l