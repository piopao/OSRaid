PRG_SUFFIX_FLAG := 0
#
LDFLAGS := -lm `pkg-config fuse --cflags --libs`
CFLAGS := -g -Wall -std=gnu99 -D_FILE_OFFSET_BITS=64
#
## ==================- NOTHING TO CHANGE BELOW THIS LINE ===================
##
SRCS := $(wildcard *.c)
PRGS := $(patsubst %.c,%,$(SRCS))
PRG_SUFFIX=.exe
BINS := $(patsubst %,%$(PRG_SUFFIX),$(PRGS))
## OBJS are automagically compiled by make.
OBJS := $(patsubst %,%.o,$(PRGS))
##
all : $(BINS)
##
## For clarity sake we make use of:
.SECONDEXPANSION:
OBJ = $(patsubst %$(PRG_SUFFIX),%.o,$@)
ifeq ($(PRG_SUFFIX_FLAG),0)
		BIN = $(patsubst %$(PRG_SUFFIX),%,$@)
else
		BIN = $@
endif
## Compile the executables
%$(PRG_SUFFIX) : $(OBJS)
	$(CC) $(OBJ)  $(LDFLAGS) -o $(BIN)
##
## $(OBJS) should be automagically removed right after linking.
##
veryclean:
ifeq ($(PRG_SUFFIX_FLAG),0)
	$(RM) $(PRGS)
else
	$(RM) $(BINS)
endif
##
rebuild: veryclean all