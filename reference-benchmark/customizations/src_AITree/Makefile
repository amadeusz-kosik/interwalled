VER=0.10-dev
RELEASE=v$(VER)

CC = gcc
CFLAGS = -Wall -ggdb -D_FILE_OFFSET_BITS=64

CFILES = AITree.c interval_tree.c rbtree.c
HEADERS = interval_tree_generic.h  interval_tree.h  rbtree_augmented.h  rbtree.h

BIN = interval-tree-test AITree
OBJ = $(CFILES:.c=.o)

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@ $(LIBRARY_FLAGS)

AITree: $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o AITree $(LIBRARY_FLAGS)

clean:
	rm -fr $(BIN) $(OBJ)
