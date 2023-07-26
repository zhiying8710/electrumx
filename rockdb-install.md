### 1. install rocksdb
```bash
sudo apt update
sudo apt install build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libzstd-dev pkg-config
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
git checkout 6.29.fb
make shared_lib
sudo make install-shared # will link rocksdb shared so file to /usr/local/lib
export LD_LIBRARY_PATH=/usr/local/lib # export rocksdb shared so file
```
### 2. install python-rocksdb
```bash
pip3 install cython pkgconfig
git clone https://github.com/HathorNetwork/python-rocksdb.git
cd python-rocksdb
python setup.py install
```