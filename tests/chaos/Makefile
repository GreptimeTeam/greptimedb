OS_NAME := $(shell uname -s | tr A-Z a-z)
ARCH := amd64
CHAOSD_VERSION := latest

ifeq ($(shell uname -p),arm)
	ARCH = arm
endif

os:
	@echo $(OS_NAME)

install-chaosd:
ifeq ($(OS_NAME), linux)
	$(MAKE) download-chaosd	
	tar zxvf ./bin/chaosd-$(CHAOSD_VERSION)-linux-$(ARCH).tar.gz -C ./bin/
	mv ./bin/chaosd-$(CHAOSD_VERSION)-linux-$(ARCH)/ ./bin/chaosd/
	rm ./bin/chaosd-$(CHAOSD_VERSION)-linux-$(ARCH).tar.gz
else
	@echo "Unsupported OS: $(OS_NAME)"
endif

download-chaosd:
	wget -P ./bin/ https://mirrors.chaos-mesh.org/chaosd-$(CHAOSD_VERSION)-linux-$(ARCH).tar.gz

install-gtctl:
	wget -P ./bin/ https://raw.githubusercontent.com/greptimeteam/gtctl/develop/hack/install.sh 
	chmod +x ./bin/install.sh
	cd ./bin && ./install.sh
	rm ./bin/install.sh

start-chaosd:
	./bin/chaosd/chaosd server --port 31767
