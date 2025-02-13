FROM gcc:13.2.0

RUN apt update && \
    apt install -y bedtools=2.30.0+dfsg-3 && \
    rm -rf /var/lib/apt/lists/*

COPY submodules/AIList /usr/src/AIList
COPY customizations /usr/src/AIList

WORKDIR /usr/src/AIList/src_AITree
RUN make clean AITree

WORKDIR /usr/src/AIList
RUN make clean ailist

ENTRYPOINT ["/usr/src/AIList/entrypoint.sh"]