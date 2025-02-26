#!/bin/bash

sudo apt-get update
sudo apt install -y wget \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libreadline-dev \
    libffi-dev \
    curl \
    libbz2-dev \
    git \
    pkg-config

cd /tmp/
wget https://www.python.org/ftp/python/3.12.0/Python-3.12.0.tgz
tar -xf Python-3.12.0.tgz
cd Python-3.12.0

./configure --enable-optimizations
make -j $(nproc)
sudo make altinstall

cd /tmp/
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash

echo 'export NVM_DIR="$HOME/.nvm"' >>~/.bashrc
echo '[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"' >>~/.bashrc
echo '[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"' >>~/.bashrc

. ~/.bashrc

nvm install node
nvm use node
