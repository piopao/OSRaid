#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

// #ifdef linux
//  For pread()/pwrite() 
// #define _XOPEN_SOURCE 500
// #endif

#include <ctype.h>

#include <stdio.h>
#include <stdlib.h>
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
// #ifdef HAVE_SETXATTR
// #include <sys/xattr.h>
// #endif


#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include "utlist.h"

#include <math.h>


#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

enum Command{GETATTR, OPEN, READ, READDIR, MKDIR, RMDIR, CREATE, UNLINK, RELEASE, WRITE, TRUNCATE};

struct server{
    char* ip;
    int port;
    int sockfd;    
};

struct mount{
    char* diskname;
    char* mountpoint;
    char* raid;
    struct server_container* server_cont;
    struct server* hotswap;
};

struct metadata{
    char* errorlog;
    char* cache_size;
    char* cache_replacment;
    char* timeout;
};

struct server_container{
    struct server** servers;
    int size;
};

struct mount_container{
    struct mount** mounts;
    int size;
};

struct metadata* meta;
struct mount_container* mt_container;


struct server_fd{
    char* ip;
    int port;
    int fd;
};

/*max 10 servers*/
typedef struct element{
    struct server_fd* server_fds[10];
    int size;
    int final_fd;
    struct element *next;
    struct element *prev;
};

struct element *head = NULL;
int global_fd_counter = 3;
int RECVLEN = 2000;
int SENDLEN = 1000;
struct mount* curmount;

int CHUNKSIZE = 10;

int ERRORCODE = -1000;
int NOFILE = -2000;


int write_int_in_buffer(int towrite, char* buffer){
    uint32_t towrite_typed = htonl(towrite);
    memcpy(buffer, (char*)&towrite_typed, sizeof(uint32_t));
    return sizeof(uint32_t);
}

int read_int_from_socket(int sockfd){
    char buf[sizeof(uint32_t)];
    recv(sockfd, buf, sizeof(uint32_t), 0);
    uint32_t intcont = *(uint32_t*)buf;
    return ntohl(intcont);
}

int read_int_from_buffer(char** buffer){
    // char buf[sizeof(uint32_t)];
    // recv(sockfd, buf, sizeof(uint32_t), 0);
    uint32_t intcont = **(uint32_t**)buffer;
    *buffer = *buffer + sizeof(uint32_t);
    return ntohl(intcont);
}

int read_string_from_socket(int sockfd, int len, char* buffer){
    int received = recv(sockfd, buffer, len, 0);
    buffer[len] = '\0';
    return received;
}

int read_string_from_buffer(char** buffer, int len, char* string){
    // int received = recv(sockfd, buffer, len, 0);
    memcpy(string, *buffer, len);
    string[len] = '\0';
    *buffer = *buffer + len;
    // return received;
    return 0;
}


int get_socket(struct server* s){
    if(s->sockfd != -1) return s->sockfd;
    struct sockaddr_in serv_addr;

    memset(&serv_addr, '0', sizeof(serv_addr));

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        return -1;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(s->port);
    if(inet_pton(AF_INET, s->ip, &serv_addr.sin_addr)<=0)
        return -1;

    if(connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        return -1;
    s->sockfd = sockfd;
    return sockfd;
}


int generate_default_prefix(int command, const char* path, char* buffer){
    int total_len = 0;
    total_len += write_int_in_buffer(command, buffer);
    total_len += write_int_in_buffer(strlen(path), buffer+total_len);
    memcpy(buffer+total_len, path, strlen(path));
    total_len += strlen(path);
    return total_len;
}

/*write -1 read 0 */
int send_data(int sockfd, char * buffer, int size){
    int total_len = 0;
    while(total_len < size){
        int bytes = write(sockfd, buffer + total_len, size - total_len);
        if (bytes < 0){
            total_len = ERRORCODE;
            break;
        } 
        total_len += bytes;
    } 
    return total_len;
}


int readdr_getattr_single(const char* path, struct stat *stbuf, int sockfd){
    memset(stbuf, '0', sizeof(struct stat));

    char buffer[1024];
    int size = generate_default_prefix(GETATTR, path, buffer);   

    int sent =  send_data(sockfd, buffer, size);
    if(sent == ERRORCODE) return ERRORCODE;


    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    } 

    int err = read_int_from_buffer(&recvbuffer);

    if (err < 0){
        free(initialbuffer);
        return err;
    } 
    if(err >= 0){
        stbuf->st_mode = (mode_t)read_int_from_buffer(&recvbuffer);
        stbuf->st_nlink = (nlink_t)read_int_from_buffer(&recvbuffer);
        stbuf->st_uid = (uid_t)read_int_from_buffer(&recvbuffer);
        stbuf->st_gid = (gid_t)read_int_from_buffer(&recvbuffer);
        stbuf->st_rdev = (dev_t)read_int_from_buffer(&recvbuffer);
        stbuf->st_size = (off_t)read_int_from_buffer(&recvbuffer);
        stbuf->st_blocks = (blkcnt_t)read_int_from_buffer(&recvbuffer);       
        stbuf->st_atime = time(NULL);
        stbuf->st_mtime = time(NULL);
        stbuf->st_ctime = time(NULL);
    }

    free(initialbuffer);
    return 0;
}


static int readdr_getattr(const char* path, struct stat *stbuf){
    printf("\n\n get attribute \n\n");
    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_getattr_single(path, stbuf, sockfd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
        else break;
    }
    printf("\n\n end of get attribute \n\n");
    return 0;
}

int get_real_size(int size, int i, int ndrives){
    int nchunks = size/CHUNKSIZE;

    int rem = size % CHUNKSIZE;

    int first_parity = ndrives - 1 - i;

    int fullcycles = (nchunks - first_parity)/ndrives;
    int remcycle = (nchunks - first_parity) % ndrives;

    int paritysize = fullcycles * CHUNKSIZE;
    paritysize += remcycle + 1 == ndrives ? rem : 0;

    return size - paritysize;
    // int nparities = (int)nchunks / ndrives;
    // nparities += (ndrives - i) <= nchunks%ndrives ? 1:0;
    // return (nchunks - nparities)*CHUNKSIZE;

}

static int readdr_getattr_raid5(const char* path, struct stat *stbuf){
    printf("\n\n get attribute raid5\n\n");
    int final_size = 0;
    int sum_size = 0;
    int not_responded = -1;

    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_getattr_single(path, stbuf, sockfd);

        if(res == ERRORCODE){
            if(not_responded >= 0){
                return -1;
            }
            not_responded = i;
            continue;
        }
        if(res < 0){
            return res;
        } 
        else{
            sum_size += stbuf->st_size;
            int realsize = get_real_size(stbuf->st_size, i, curmount->server_cont->size);
            final_size += realsize;
        }
    }

    if(not_responded >= 0){
        final_size += get_real_size(sum_size/curmount->server_cont->size, not_responded, curmount->server_cont->size);
    }
    stbuf->st_size = final_size;
    printf("final size%d\n", final_size);
    printf("\n\n end of get attribute raid5\n\n");
    return 0;
}



int readdr_readdir_single(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, int sockfd){
    char buffer[1024];
    int size = generate_default_prefix(READDIR, path, buffer);   
    int sent = send_data(sockfd, buffer, size);

    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }
    int count = read_int_from_buffer(&recvbuffer);

    int err = read_int_from_buffer(&recvbuffer);
    if(err < 0){
        free(initialbuffer);
        return err;
    }
    
    for(int i=0; i<count; i++){
        ino_t d_ino = (ino_t)read_int_from_buffer(&recvbuffer);

        unsigned char d_type = (unsigned char)read_int_from_buffer(&recvbuffer);


        int len = read_int_from_buffer(&recvbuffer);

        char d_name[1000];

        read_string_from_buffer(&recvbuffer, len, d_name);
        d_name[len] = '\0';

        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = d_ino;
        st.st_mode = d_type;
        if (filler(buf, d_name, &st, 0))
            break;
    }

    free(initialbuffer);

    return 0;


}

static int readdr_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi){ 
    printf("\n\n readdir \n\n");

    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_readdir_single(path, buf, filler, offset, fi, sockfd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
        else break;
    }

    printf("\n\n end of readdir \n\n");

    return 0;
    
}


int fdcmp(struct element *a, struct element *b) {
    if(a->final_fd == b->final_fd)
        return 0;
    return 1;
}

struct element* get_fd_element(int fd){
    struct element elem;
    struct element* found;
    elem.final_fd = fd;

    DL_SEARCH(head,found,&elem,fdcmp);
    return found;
}


int add_element_fd(struct server_fd* serv, int final_fd){
    if(final_fd == NOFILE){
        struct element* elem = malloc(sizeof(struct element));
        elem->server_fds[0] = serv;
        elem->size = 1;
        elem->final_fd = global_fd_counter;
        global_fd_counter += 1;
        DL_APPEND(head, elem);
        return elem->final_fd;
    }else{
        struct element* found = get_fd_element(final_fd);
        if(found != NULL){
            found->server_fds[found->size]=serv;
            found->size ++;
            return found->final_fd;
        }
    } 
    return -1;
}


int readdr_open_single(const char *path, struct fuse_file_info *fi, int sockfd, struct server* server, int* globalfd){
    char buffer[1024];
    int size = generate_default_prefix(OPEN, path, buffer);  
    size += write_int_in_buffer((int)fi->flags, buffer+size);
    int sent = send_data(sockfd, buffer, size);
    if(sent == ERRORCODE) return ERRORCODE;


    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }

    int fd = read_int_from_buffer(&recvbuffer);

    fflush(stdout);
    if(fd < 0){
        free(initialbuffer);
        return fd;
    }
    // int fd = read_int_from_socket(sockfd);

    struct server_fd* tmp = malloc(sizeof(struct server_fd));
    tmp->ip = server->ip;
    tmp->port = server->port;
    tmp->fd = fd;

    fi->fh = add_element_fd(tmp, *globalfd);
    *globalfd = fi->fh;
    free(initialbuffer);

    printf("open server port %d got fd %d\n\n", server->port, tmp->fd);
   
    return 0;
}



static int readdr_open(const char *path, struct fuse_file_info *fi){
    printf("\n\n open \n\n");
    int* fd = malloc(sizeof(int));
    *fd = NOFILE;
    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_open_single(path, fi, sockfd, curmount->server_cont->servers[i], fd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
    }
    printf("\n\n end of open \n\n");
    return 0;
    
}



int readdr_create_single(const char *path, mode_t mode,  struct fuse_file_info *fi, int sockfd, struct server* server, int* globalfd){
    char buffer[1024];
    int size = generate_default_prefix(CREATE, path, buffer);  
    size += write_int_in_buffer((int)fi->flags, buffer+size);
    size += write_int_in_buffer(mode, buffer+size);
    int sent = send_data(sockfd, buffer, size);
    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }

    int fd = read_int_from_buffer(&recvbuffer);

   
    if(fd < 0){
        free(initialbuffer);
        return fd;
    }

    struct server_fd* tmp = malloc(sizeof(struct server_fd));
    tmp->ip = server->ip;
    tmp->port = server->port;
    tmp->fd = fd;

    fi->fh = add_element_fd(tmp, *globalfd);
    *globalfd = fi->fh;
    free(initialbuffer);
    return 0;
}



static int readdr_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    printf("\n\n create \n\n");
    int* fd = malloc(sizeof(int));
    *fd = NOFILE;
    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_create_single(path, mode, fi, sockfd, curmount->server_cont->servers[i], fd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
    }
    printf("\n\n end of create \n\n");
    return 0;
}


int get_server_fd(struct element* fdelem, struct server* server){
    //todo: fdelem exa davamate ==NULL;
    if(fdelem == NULL){
        return -1;
    } 
    // int size = sizeof(fdelem->server_fds)/sizeof(fdelem->server_fds[0]);
    // fflush(stdout);
    for(int i=0; i < fdelem->size; i++){
        fflush(stdout);
        struct server_fd* tmp = fdelem->server_fds[i];
        if(strcmp(tmp->ip, server->ip) == 0 && tmp->port == server->port){
            return tmp->fd;
        }
    }
    return -1;
}




int readdr_mkdir_single(const char *path, mode_t mode, int sockfd){
    char buffer[1024];
    int send_size = generate_default_prefix(MKDIR, path, buffer); 
    send_size += write_int_in_buffer(mode, buffer+send_size);

    int sent = send_data(sockfd, buffer, send_size);
    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }

    int res = read_int_from_buffer(&recvbuffer);
    free(initialbuffer);
    return res;
}

static int readdr_mkdir(const char *path, mode_t mode){
    printf("\n\n mkdir \n\n");
    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_mkdir_single(path, mode, sockfd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
    }
    printf("\n\n end of mkdir \n\n");
    return 0;
    
}


int readdr_rmdir_single(const char*path, int sockfd){
    char buffer[1024];
    int send_size = generate_default_prefix(RMDIR, path, buffer); 
    int sent = send_data(sockfd, buffer, send_size);
    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }

    int res = read_int_from_buffer(&recvbuffer);
    free(initialbuffer);

    return res;

}


static int readdr_rmdir(const char *path){
    printf("\n\n rmdir \n\n");
    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_rmdir_single(path, sockfd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
    }
    printf("\n\n end of rmdir \n\n");
    return 0;    
}



int readdr_unlink_single(const char *path, int sockfd){
    char buffer[1024];
    int size = generate_default_prefix(UNLINK, path, buffer);  
    int sent = send_data(sockfd, buffer, size);
    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }

    int res = read_int_from_buffer(&recvbuffer);
    free(initialbuffer);

    return res;
}




static int readdr_unlink(const char *path){
    printf("\n\n unlink \n\n");

    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_unlink_single(path, sockfd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
    }
    printf("\n\n end of unlink \n\n");

    return 0;     
}



int readdr_release_single(const char *path, struct fuse_file_info *fi, int sockfd, struct element* elem, struct server* server){
    char buffer[1024];
    int size = generate_default_prefix(RELEASE, path, buffer);
    // int first_server_fd = elem->server_fds[0]->fd;
    int first_server_fd = get_server_fd(elem, server);
    if(first_server_fd == -1) {
        return ERRORCODE;
    }
    
    size += write_int_in_buffer(first_server_fd, buffer+size);
    send_data(sockfd, buffer, size);

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return -1;
    }

    int res = read_int_from_buffer(&recvbuffer);
    free(initialbuffer);
    return res;
}



static int readdr_release(const char *path, struct fuse_file_info *fi){
    printf("\n\n release \n\n");

    struct element* elem = get_fd_element(fi->fh);
    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_release_single(path, fi, sockfd, elem, curmount->server_cont->servers[i]);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
    }
    DL_DELETE(head,elem);
    printf("\n\n end of release \n\n");

    return 0;
    
}

int readdr_read_single(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi, int sockfd, struct server* server){

    struct element* fdelem =  get_fd_element(fi->fh);
    int first_server_fd = get_server_fd(fdelem, server);

    printf("first_server_fd %d server port %d \n\n", first_server_fd, server->port);
    if(first_server_fd < 0) return ERRORCODE;
    // int first_server_fd = fdelem->server_fds[0]->fd;
    
    char buffer[1024];
    int send_size = generate_default_prefix(READ, path, buffer); 
    send_size += write_int_in_buffer(first_server_fd, buffer+send_size);

    printf("read single size %d\n", size);
    send_size += write_int_in_buffer(size, buffer+send_size);
    send_size += write_int_in_buffer(offset, buffer+send_size);
    int sent = send_data(sockfd, buffer, send_size);
    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);

    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }


    int res = read_int_from_buffer(&recvbuffer);
    printf("read first int from buffer in single read%d\n", res);

    if(res < 0 ){
        free(initialbuffer);
        return res;
    }
    int len = read_int_from_buffer(&recvbuffer);
    printf("read recv text length in single read%d\n", len);

    read_string_from_buffer(&recvbuffer, len, buf);

    //todo: amas rame vuyo, 1 simboloze chedams
    // free(initialbuffer);
    return len;
}


int readdr_read_single_wrapper(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi, int sockfd,  struct server* server){
    printf("read single wrapper\n");
    int readbytes = 0;
    while(readbytes < size){
        int bytes_to_read = size - readbytes < SENDLEN ? size - readbytes : SENDLEN;
        printf("bytes to read %d\n", bytes_to_read);
        printf("offset%d\n", offset);
        int res = readdr_read_single(path, buf + readbytes, bytes_to_read, offset, fi, sockfd, server);
        if(res == ERRORCODE) return ERRORCODE;
        if(res <= 0) return res;
        readbytes += res;
        if(res < bytes_to_read) return readbytes;
        offset += res;
    }
    printf("finished with read %d\n", readbytes);
    return readbytes;
}



static int readdr_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    for (int i=0; i < curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_read_single_wrapper(path, buf, size, offset, fi, sockfd, curmount->server_cont->servers[i]);
        if(res == ERRORCODE) continue;
        return res;
    }
    return 0;
}





int readdr_write_single(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi, int sockfd){
    char buffer[size + 100];
    int final_size = generate_default_prefix(WRITE, path, buffer);
    struct element* elem = get_fd_element(fi->fh);
    int first_server_fd = elem->server_fds[0]->fd;

    final_size += write_int_in_buffer(first_server_fd, buffer+final_size);
    final_size += write_int_in_buffer(offset, buffer+final_size);
    final_size += write_int_in_buffer(size, buffer+final_size);
    memcpy(buffer+final_size, buf, size);
    final_size += size;

    int sent = send_data(sockfd, buffer, final_size);
    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }
    int res = read_int_from_buffer(&recvbuffer);

    free(initialbuffer);

    return res;
}


int readdr_write_single_wrapper(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi, int sockfd){
    int sentbytes = 0;
    while(sentbytes < size){
        int bytes_to_send = size - sentbytes < SENDLEN ? size - sentbytes : SENDLEN;
        int res = readdr_write_single(path, buf + sentbytes, bytes_to_send, offset, fi, sockfd);
        if(res == ERRORCODE) return ERRORCODE;
        if(res <= 0) return res;
        sentbytes += res;
    }
    return sentbytes;
}



static int readdr_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int res;
    for (int i=0; i< curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        res = readdr_write_single_wrapper(path, buf, size, offset, fi, sockfd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;

    }
    return res;    
}



char* get_stripe_parity(const char* buf, int stripe, int size){
    int server_count = curmount->server_cont->size;
    char* parity = malloc(CHUNKSIZE);
    for(int i=0; i<CHUNKSIZE; i++){
        char ch = (char)0;
        for(int j=0; j<server_count; j++){
            int offs = stripe*(server_count-1)*CHUNKSIZE + j*CHUNKSIZE + i;
            if(offs > size) ch = ch^((char)0);
            else ch = ch^buf[offs];
        }
        parity[i] = ch;
    }
    return parity;
}


static int readdr_read_raid5_tmp(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    printf("\n\n read raid 5\n\n");
    printf("read size %d\n\n", size);
    int nblocks =  (int)ceil((float)size/CHUNKSIZE);
    printf("nblocks %d\n", nblocks);
    char* tmpbuffer = malloc(CHUNKSIZE+1);
    tmpbuffer[CHUNKSIZE] = '\0';
    int server_count = curmount->server_cont->size;
    int readbytes = 0;
    memset(buf, 0, size);
    for(int i=0; i<nblocks; i++){
        int servNum = i % server_count;
        int offsetNum = (int)ceil((float)i/(server_count-1));
        printf("i %d servernum %d offsetNum %d\n", i, servNum, offsetNum);

        int sockfd = get_socket(curmount->server_cont->servers[servNum]);
        int res = readdr_read_single_wrapper(path, tmpbuffer, CHUNKSIZE, offsetNum*CHUNKSIZE, fi, sockfd, curmount->server_cont->servers[servNum]);
        printf("read %s nbytes %d \n", tmpbuffer, res);

        if(res == ERRORCODE){
            printf("\n errorcode read bytes %d\n", readbytes);
            free(tmpbuffer);
            return readbytes;
        }  //tmp;
        if(res <= 0){
            printf("\n res < 0 readbytes bytes %d\n", readbytes);
            free(tmpbuffer);

            return readbytes;
        } 
        memcpy(buf+i*CHUNKSIZE, tmpbuffer, CHUNKSIZE);
        buf[size]='\0';
        printf("read %s in buffer buffer looks like this \n", buf);

        readbytes += CHUNKSIZE;

    }
    free(tmpbuffer);
    printf("\n\n end of read raid 5\n\n");
    printf("\n read bytes %d\n", readbytes);
    return readbytes;
}

char* xor_buffers(char* buf1, char* buf2, int size){
    char* parity = malloc(size);
    for(int i=0; i<size; i++){
        char ch = (char)0;
        ch = buf1[i]^buf2[i];
        parity[i] = ch;
    }
    return parity;
}


static int readdr_read_raid5(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    printf("read raid 5 %d bytes from %d\n", size, offset);
    int server_count = curmount->server_cont->size;
    int nblock = (int)offset / CHUNKSIZE;

    int curoffset = offset % CHUNKSIZE;
    int read_size = 0;

    while(read_size < size){
        int servNum = nblock % server_count;
        printf("\n\nservnum, WHICHSERVER %d\n\n", servNum);
        int stripe = (int)nblock/(server_count-1);
        int write_size = MIN(CHUNKSIZE - curoffset, size - read_size);
        printf("CHUNKSIZE%d CUROFFSET%d SIZE%d READSIZE%d\n ", CHUNKSIZE, curoffset, size, read_size);
        int sockfd = get_socket(curmount->server_cont->servers[servNum]);

        // int parityServerNum = server_count-1 - stripe%(server_count);
        // int parity_sockfd = get_socket(curmount->server_cont->servers[parityServerNum]);

        char* tmpbuf_readblock = malloc(write_size);
        memset(tmpbuf_readblock, 0, write_size);

        int res = readdr_read_single_wrapper(path, tmpbuf_readblock, write_size, stripe*CHUNKSIZE+curoffset, fi, sockfd, curmount->server_cont->servers[servNum]);

        printf("raid5 read res, writesize %d %d\n", res, write_size);
        if(res < 0){ 
            free(tmpbuf_readblock);
            return res;
        } 
        memcpy(buf+read_size, tmpbuf_readblock, res);

        read_size += res;

        if(res < write_size){          
            printf("read this to buf %s\n", buf);
            free(tmpbuf_readblock);
            printf("morcha yvelaferi %d\n", read_size); 
            return read_size; 
        } 
        
        nblock++;
        curoffset = 0;

        free(tmpbuf_readblock);

    }
    return read_size;
}



static int readdr_write_raid5(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    printf("raid5 write \n");
    int server_count = curmount->server_cont->size;
    int nblock = (int)offset / CHUNKSIZE;

    int curoffset = offset % CHUNKSIZE;
    int written_size = 0;

    while(written_size < size){
        int servNum = nblock % server_count;
        int stripe = (int)nblock/(server_count-1);
        int write_size = MIN(CHUNKSIZE - curoffset, size - written_size);
        int sockfd = get_socket(curmount->server_cont->servers[servNum]);

        int parityServerNum = server_count-1 - stripe%(server_count);
        int parity_sockfd = get_socket(curmount->server_cont->servers[parityServerNum]);

        char* tmpbuf_readblock = malloc(write_size);
        char* tmpbuf_parityblock = malloc(write_size);
        memset(tmpbuf_readblock, 0, write_size);
        memset(tmpbuf_parityblock, 0, write_size);

        int res = readdr_read_raid5(path, tmpbuf_readblock, write_size, offset+written_size, fi);

        res = readdr_read_single_wrapper(path, tmpbuf_parityblock, write_size, stripe*CHUNKSIZE+curoffset, fi, sockfd, curmount->server_cont->servers[parityServerNum]);
        char* old_xor = xor_buffers(tmpbuf_readblock, tmpbuf_parityblock, write_size);
        char* new_xor = xor_buffers(old_xor, buf+written_size, write_size);

        res = readdr_write_single_wrapper(path, new_xor, write_size, stripe*CHUNKSIZE+curoffset, fi, parity_sockfd);
        res = readdr_write_single_wrapper(path, buf+written_size, write_size, stripe*CHUNKSIZE+curoffset, fi, sockfd);

        written_size += res;
        
        nblock++;
        curoffset = 0;

        free(old_xor);
        free(new_xor);
        free(tmpbuf_readblock);
        free(tmpbuf_parityblock);

    }
    printf("raid5 end of write \n");
    return written_size;
    
}



static int readdr_write_raid5_tmp(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {  
    printf("\n\n write raid 5\n\n");
    int nstripes =  (int)ceil(((float)size) / (CHUNKSIZE*(curmount->server_cont->size-1)));
    int server_count = curmount->server_cont->size;
    int fail_count = 0;
    char* tosend = malloc(CHUNKSIZE);
    memset(tosend, 0, CHUNKSIZE);
    for(int i=0; i<nstripes; i++){
        for(int j=0; j<server_count-1; j++){
            int nblock = i*(server_count-1)+j;
            int servNum = nblock % server_count;
            int offsetNum = (int)ceil(nblock/(server_count-1));

            printf("nblock %d servNum %d offsetNum %d \n", nblock, servNum, offsetNum);
            int sockfd = get_socket(curmount->server_cont->servers[servNum]);

            memset(tosend, 0, CHUNKSIZE);
            int copysize = MIN(CHUNKSIZE, size - (nblock-1*CHUNKSIZE));
            printf("copysize %d \n", copysize);
            if(copysize <= 0){
                break;
            }
            memcpy(tosend, buf+nblock*CHUNKSIZE, copysize);
            printf("char to print %c\n", tosend[0]);
            int res = readdr_write_single_wrapper(path, tosend, CHUNKSIZE, offsetNum*CHUNKSIZE, fi, sockfd);
            if(res == ERRORCODE){
                fail_count++;
                if(fail_count > 1) return -1;
                continue;
            } 
            if(res < 0) return res;
        }
        int parityIndex = server_count-1 - i%(server_count);
        printf("parity index %d\n", parityIndex);
        printf("paritfy column %d\n", i);

        int sockfd = get_socket(curmount->server_cont->servers[parityIndex]);
        char* parity = get_stripe_parity(buf, i, size);
        printf("parity char %c\n", parity[0]);

        int res = readdr_write_single_wrapper(path, parity, CHUNKSIZE, i*CHUNKSIZE, fi, sockfd);
        if(res == ERRORCODE){
            fail_count++;
            if(fail_count > 1) return -1;
            continue;
        } 
        if(res < 0) return res;
        fail_count = 0;
    }
    printf("\n\nend of write raid 5\n\n");
    return size;
}




static int readdr_utimens(const char*path, const struct timespec ts[2]){
    return 0;
}


int save_metadata(char* data[4]){
    meta = malloc(sizeof(struct metadata));
    char delim[2];
    delim[0] = '=';
    delim[1] = ' ';
    char *token;

    token = strtok(data[0], delim);
    token = strtok(NULL, delim);
    meta->errorlog = strdup(token);

    token = strtok(data[1], delim);
    token = strtok(NULL, delim);
    meta->cache_size = strdup(token);

    token = strtok(data[2], delim);
    token = strtok(NULL, delim);
    meta->cache_replacment = strdup(token);

    token = strtok(data[3], delim);
    token = strtok(NULL, delim);
    meta->timeout = strdup(token);

    return 1;

}


struct server* get_server(char* serv_pair){

    char *token;
   
    char delim[1];
    delim[0] = ':';

    char *rest = serv_pair;

    token = strtok_r(rest, delim, &rest);

    struct server* tmp = malloc(sizeof(struct server));


    // token= strtok_r(rest, delim, &rest);
    tmp->ip = strdup(token);

    token = strtok_r(rest, delim, &rest);

    int port = strtol(token, (char **)NULL, 10);
    tmp->port = port;
    tmp->sockfd = -1;

    return tmp;

}

int get_servers(char* line, struct mount* mount){

    struct server_container* s = malloc(sizeof(struct server_container));
    s->servers = malloc(sizeof(struct server*));
    s->size = 0;

    char delim[3];
    delim[0] = ' ';
    delim[1] = ',';
    delim[2] = '=';

    char *token;
    char *rest = line;

    token = strtok_r(rest, delim, &rest);
    token = strtok_r(rest, delim, &rest);

    while( token != NULL ) {
        struct server* stmp = get_server(token);
        if(s->size == 0){
            s->servers[0] = stmp;

        }else{
            s->servers = (struct server**)realloc(s->servers, sizeof(struct server*)*(s->size+1));
            s->servers[s->size] = stmp;
        }
        s->size++;
        token = strtok_r(rest, delim, &rest);
    }
    mount->server_cont = s;
    return 1;
}


int save_mount_data(FILE* fp){

    mt_container = malloc(sizeof(struct mount_container));
    mt_container->mounts = malloc(sizeof(struct mount*));

    mt_container->size = 0;
    char* token;

    char * line = NULL;
    size_t len = 0;
    ssize_t read; 

    while ((read = getline(&line, &len, fp)) != -1) {
        if('\n'== line[0]){
            read = getline(&line, &len, fp);
            if('\n' == line[0]) break;         
        }
        struct mount* mt = malloc(sizeof(struct mount));

        char delim[3];
        delim[0] = '=';
        delim[1] = ' ';
        delim[2] = '\n';
        token = strtok(line, delim);
        token = strtok(NULL, delim);
        mt->diskname = strdup(token);

        read = getline(&line, &len, fp);

        token = strtok(line, delim);
        token = strtok(NULL, delim);

        mt->mountpoint = strdup(token);

        read = getline(&line, &len, fp);

        token = strtok(line, delim);
        token = strtok(NULL, delim);
        mt->raid = strdup(token);

        read = getline(&line, &len, fp);

        get_servers(line, mt);
      
        read = getline(&line, &len, fp);

        token = strtok(line, delim);
        token = strtok(NULL, delim);
        mt->hotswap = get_server(token);

        if(mt_container->size == 0){
            mt_container->mounts[0] = mt;
        }else{
            mt_container->mounts = (struct mount**)realloc(mt_container->mounts, sizeof(struct mount*)*(mt_container->size+1));
            mt_container->mounts[mt_container->size] = mt;
        }
        mt_container->size ++;
    }
    if (line)
        free(line);
    return 1;
}


struct mount* parse_config(char* filename){
    FILE * fp;
    char * line = NULL;
    size_t len = 0;
    ssize_t read;

    char cwd[1024];
    getcwd(cwd, sizeof(cwd));
    char* slash = "/";

    char* name_with_extension = malloc(strlen(cwd)+strlen(filename)+strlen(slash)+1);
    strcpy(name_with_extension, cwd);
    strcat(name_with_extension, slash);
    strcat(name_with_extension, filename);


    fp = fopen(name_with_extension, "r");
    if (fp == NULL)
        return NULL;

    char* metadata_lines[4];


    for(int i=0; i<4; i++){
        read = getline(&line, &len, fp);
        metadata_lines[i] = strdup(line);
    }

    save_metadata(metadata_lines);


    save_mount_data(fp);

    fclose(fp);
    if (line)
        free(line);
    return NULL;
}




int readdr_truncate_single(const char*path, off_t size, int sockfd){
    char buffer[1000];
    int final_size = generate_default_prefix(TRUNCATE, path, buffer);
    final_size += write_int_in_buffer(size, buffer+final_size);
    int sent = send_data(sockfd, buffer, final_size);
    if(sent == ERRORCODE) return ERRORCODE;

    char* recvbuffer = malloc(2000);
    char* initialbuffer = recvbuffer;
    int recieved = read(sockfd, recvbuffer, 2000);
    if(recieved == -1){
        free(initialbuffer);
        return ERRORCODE;
    }
    int res = read_int_from_buffer(&recvbuffer);

    free(initialbuffer);

    printf("result from truncate %d \n", res);
    fflush(stdout);
    return res;




}
static int readdr_truncate(const char *path, off_t size){
    printf("truncate\n");
    for (int i=0; i< curmount->server_cont->size; i++){
        int sockfd = get_socket(curmount->server_cont->servers[i]);
        int res = readdr_truncate_single(path, size, sockfd);
        if(res == ERRORCODE) continue;
        if(res < 0) return res;
    }
    printf("truncate\n");  
    return 0;
}



/*mkdir unlink rmdir open write release create */
/*readdr = re-address*/
static struct fuse_operations readdr_oper = {
    .getattr    = readdr_getattr,
    .mkdir      = readdr_mkdir,
    .unlink     = readdr_unlink,
    .rmdir      = readdr_rmdir,
    .open       = readdr_open,
    .read       = readdr_read,
    .write      = readdr_write,
    .release    = readdr_release,
    // .opendir    = readdr_opendir,
    // .releasedir    = readdr_releasedir,
    .create    = readdr_create,
    .readdir   = readdr_readdir,
    .utimens = readdr_utimens,
    .truncate = readdr_truncate
};


static struct fuse_operations readdr_oper_raid5 = {
    .getattr    = readdr_getattr_raid5,
    .mkdir      = readdr_mkdir,
    .unlink     = readdr_unlink,
    .rmdir      = readdr_rmdir,
    .open       = readdr_open,
    .read       = readdr_read_raid5,
    .write      = readdr_write_raid5,
    .release    = readdr_release,
    // .opendir    = readdr_opendir,
    // .releasedir    = readdr_releasedir,
    .create    = readdr_create,
    .readdir   = readdr_readdir,
    .utimens = readdr_utimens,
    .truncate = readdr_truncate
};




int main(int argc, char *argv[])
{
    umask(0);
    parse_config(argv[1]);
    // int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    // receive_data(sockfd);
    // return 0;
    // printf("%s mountpointiii\n", mt_container->mounts[0]->mountpoint);
    curmount = mt_container->mounts[1];
    // curmount->server_cont->size = 1;
    argv[1] = curmount->mountpoint;
    if(strcmp(curmount->raid, "5")==0){
        printf("raid5");
        return fuse_main(argc, argv, &readdr_oper_raid5, NULL);
    }
    else return fuse_main(argc, argv, &readdr_oper, NULL);


    // for (int i=0; i<1;){
    //     int par = fork();
    //     if(par == 0){
    //         curmount = mt_container->mounts[i];
    //         argv[1] = curmount->mountpoint;
    //         return fuse_main(argc, argv, &readdr_oper, NULL);
    //     }else{
    //         i += 1;
    //     }
    // }

    return 0;   
}
