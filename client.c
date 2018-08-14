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



enum Command{GETATTR, OPEN, READ, READDIR, MKDIR, RMDIR, CREATE, UNLINK, RELEASE, WRITE};

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
    int server_num;
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

struct mount* curmount;

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

int read_string_from_socket(int sockfd, int len, char* buffer){
    int received = recv(sockfd, buffer, len, 0);
    buffer[len] = '\0';
    return received;
}


int get_socket(struct server* s){
    struct sockaddr_in serv_addr;
    
    memset(&serv_addr, '0', sizeof(serv_addr));

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        return -1;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(s->port);
    if(inet_pton(AF_INET, s->ip, &serv_addr.sin_addr)<=0)
        return -1;

    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        return -1;

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


int send_data(int sockfd, char * buffer, int size){
    int total_len = 0;
    while(total_len < size){
        int bytes = write(sockfd, buffer + total_len, size - total_len);
        if (bytes < 0) break;
        total_len += bytes;
    } 
    return total_len;
}



static int readdr_getattr(const char* path, struct stat *stbuf){
    printf("getattr\n");

    memset(stbuf, '0', sizeof(struct stat));
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    char buffer[1024];
    int size = generate_default_prefix(GETATTR, path, buffer);   
    send_data(sockfd, buffer, size);

    int err = read_int_from_socket(sockfd);

    if (err < 0) return err;
    if(err >= 0){
        stbuf->st_mode = (mode_t)read_int_from_socket(sockfd);
        stbuf->st_nlink = (nlink_t)read_int_from_socket(sockfd);
        stbuf->st_uid = (uid_t)read_int_from_socket(sockfd);
        stbuf->st_gid = (gid_t)read_int_from_socket(sockfd);
        stbuf->st_rdev = (dev_t)read_int_from_socket(sockfd);
        stbuf->st_size = (off_t)read_int_from_socket(sockfd);
        stbuf->st_blocks = (blkcnt_t)read_int_from_socket(sockfd);        
        stbuf->st_atime = time(NULL);
        stbuf->st_mtime = time(NULL);
        stbuf->st_ctime = time(NULL);
    }
    return 0;
}


static int readdr_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi){ 
    printf("readdir plz\n");

    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    char buffer[1024];
    int size = generate_default_prefix(READDIR, path, buffer);   
    send_data(sockfd, buffer, size);

    int count = read_int_from_socket(sockfd);
    int err = read_int_from_socket(sockfd);
    if(err < 0){
        return err;
    }
    
    for(int i=0; i<count; i++){
        ino_t d_ino = (ino_t)read_int_from_socket(sockfd);
        unsigned char d_type = (unsigned char)read_int_from_socket(sockfd);
        int len = read_int_from_socket(sockfd);
        char d_name[1000];

        read_string_from_socket(sockfd, len, d_name);
        d_name[len] = '\0';
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = d_ino;
        st.st_mode = d_type;
        if (filler(buf, d_name, &st, 0))
            break;
    }

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
    if(final_fd == -1){
        struct element* elem = malloc(sizeof(struct element));
        elem->server_fds[0] = serv;
        elem->size = 1;
        elem->final_fd = global_fd_counter;
        global_fd_counter += 1;
        printf("counter plus ragaca\n");
        DL_APPEND(head, elem);
        printf("movxerxdit appendshi rogrgac\n");

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



static int readdr_open(const char *path, struct fuse_file_info *fi){
    printf("open\n");
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    char buffer[1024];
    int size = generate_default_prefix(OPEN, path, buffer);  
    size += write_int_in_buffer((int)fi->flags, buffer+size);
    send_data(sockfd, buffer, size);
    int fd = read_int_from_socket(sockfd);

    printf("open result ager %d\n" ,fd);
    fflush(stdout);
    if(fd < 0){
        return fd;
    }
    // int fd = read_int_from_socket(sockfd);

    struct server_fd* tmp = malloc(sizeof(struct server_fd));
    tmp->ip = mt_container->mounts[0]->server_cont->servers[0]->ip;
    tmp->fd = fd;

    fi->fh = add_element_fd(tmp, -1);
    printf("open closed %d\n", fi->fh);
   
    return 0;
}

static int readdr_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    printf("create\n");
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    char buffer[1024];
    int size = generate_default_prefix(CREATE, path, buffer);  
    size += write_int_in_buffer((int)fi->flags, buffer+size);
    size += write_int_in_buffer(mode, buffer+size);
    send_data(sockfd, buffer, size);


    int fd = read_int_from_socket(sockfd);

   
    if(fd < 0){
        return fd;
    }

    struct server_fd* tmp = malloc(sizeof(struct server_fd));
    tmp->ip = mt_container->mounts[0]->server_cont->servers[0]->ip;
    tmp->fd = fd;
    fi->fh = add_element_fd(tmp, -1);

    return 0;
}


static int readdr_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    printf("read \n");

    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);

    struct element* fdelem =  get_fd_element(fi->fh);

    int first_server_fd = fdelem->server_fds[0]->fd;

    char buffer[1024];
    int send_size = generate_default_prefix(READ, path, buffer); 
    send_size += write_int_in_buffer(first_server_fd, buffer+send_size);

    send_size += write_int_in_buffer(size, buffer+send_size);
    send_size += write_int_in_buffer(offset, buffer+send_size);
    send_data(sockfd, buffer, send_size);

    int res = read_int_from_socket(sockfd);

    if(res < 0 )return res;
    int len = read_int_from_socket(sockfd);
    read_string_from_socket(sockfd, len, buf);

    return len;

}

static int readdr_mkdir(const char *path, mode_t mode){
    printf("mkdir \n");
    char buffer[1024];
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    int send_size = generate_default_prefix(MKDIR, path, buffer); 
    send_size += write_int_in_buffer(mode, buffer+send_size);

    send_data(sockfd, buffer, send_size);

    int res = read_int_from_socket(sockfd);

    return res;
}

static int readdr_rmdir(const char *path){
    printf("rmdir \n");
    char buffer[1024];
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    int send_size = generate_default_prefix(RMDIR, path, buffer); 
    send_data(sockfd, buffer, send_size);

    int res = read_int_from_socket(sockfd);
    return res;
}



static int readdr_unlink(const char *path){
    printf("unlink \n");
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    char buffer[1024];
    int size = generate_default_prefix(UNLINK, path, buffer);  
    send_data(sockfd, buffer, size);
    return read_int_from_socket(sockfd);
}

static int readdr_release(const char *path, struct fuse_file_info *fi){
    printf("release \n");
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    char buffer[1024];
    int size = generate_default_prefix(RELEASE, path, buffer);

    printf("fis fh releaseshi %d\n", fi->fh);


    struct element* elem = get_fd_element(fi->fh);
    int first_server_fd = elem->server_fds[0]->fd;

    DL_DELETE(head,elem);

    size += write_int_in_buffer(first_server_fd, buffer+size);
    send_data(sockfd, buffer, size);
    printf("released sent\n");

    int res = read_int_from_socket(sockfd);
    printf("result from release %d \n", res);


    return res;

}

static int readdr_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

    printf("write\n");
    fflush(stdout);
    int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    char buffer[size + 100];
    int final_size = generate_default_prefix(WRITE, path, buffer);
    struct element* elem = get_fd_element(fi->fh);
    int first_server_fd = elem->server_fds[0]->fd;
    final_size += write_int_in_buffer(first_server_fd, buffer+final_size);
    final_size += write_int_in_buffer(offset, buffer+final_size);
    final_size += write_int_in_buffer(size, buffer+final_size);
    memcpy(buffer+final_size, buf, size);
    final_size += size;

    send_data(sockfd, buffer, final_size);

    int res = read_int_from_socket(sockfd);
    printf("result from release %d \n", res);
    fflush(stdout);
    return res;

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

    return tmp;

}

int get_servers(char* line, struct mount* mount){

    // printf("%s ORIGINAL DISKNAME IN THE BEGINNING\n", mount->diskname);

    struct server_container* s = malloc(sizeof(struct server_container));
    s->servers = malloc(sizeof(struct server*));
    s->size = 0;

    char delim[3];
    delim[0] = ' ';
    delim[1] = ',';
    delim[2] = '=';

    // printf("%s ORIGINAL DISKNAME IN THE MERE\n", mount->diskname);

    char *token;
    char *rest = line;

    token = strtok_r(rest, delim, &rest);
    token = strtok_r(rest, delim, &rest);

    // token = strtok(NULL, delim);
    // printf("%s martlatoken\n", token);

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
    .readdir   = readdr_readdir
};





int main(int argc, char *argv[])
{
    umask(0);
    parse_config(argv[1]);
    // int sockfd = get_socket(mt_container->mounts[0]->server_cont->servers[0]);
    // receive_data(sockfd);
    // return 0;
    // printf("%s mountpointiii\n", mt_container->mounts[0]->mountpoint);
    curmount = mt_container->mounts[0];
    argv[1] = curmount->mountpoint;
    return fuse_main(argc, argv, &readdr_oper, NULL);


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
