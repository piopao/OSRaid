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
#include <unistd.h>



enum command{GETATTR, OPEN, READ, READDIR, MKDIR, RMDIR, CREATE, UNLINK, RELEASE, WRITE, TRUNCATE};
char serverpath[1024];

/*write -1 read 0 fail*/
int read_int_from_buffer(char** buffer){
    // char buf[sizeof(uint32_t)];
    // recv(sockfd, buf, sizeof(uint32_t), 0);
    uint32_t intcont = **(uint32_t**)buffer;
    *buffer = *buffer + sizeof(uint32_t);
    return ntohl(intcont);
}

int read_string_from_buffer(char** buffer, int len, char* string){
    // int received = recv(sockfd, buffer, len, 0);
    memcpy(string, *buffer, len);
    string[len] = '\0';
    *buffer = *buffer + len;
    // return received;
    return 0;
}

int read_string_from_socket(int sockfd, int len, char* buffer){
    int received = recv(sockfd, buffer, len, 0);
    return received;
}

int write_string_in_buffer(char* str, char* buffer){
    strcpy(buffer, str);
    return strlen(str);
}


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

int send_data(int sockfd, char * buffer, int size){
    int total_len = 0;
    while(total_len < size){
        int bytes = write(sockfd, buffer + total_len, size - total_len);
        if (bytes < 0) break;
        total_len += bytes;
    } 
    return total_len;
}



int get_listener_socket(int port){
    int listenfd = 0;
    struct sockaddr_in serv_addr;    

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&serv_addr, '0', sizeof(serv_addr));
    
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(port); 

    int optval = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));

    bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)); 

    listen(listenfd, 10); 

    return listenfd;
}


int serve_getattr(int sockfd, char* path){
    printf("getattr path %s \n", path);
    struct stat file_stat;
    char tosend_buffer[1000];
    int tosend_size = 0;
    int st = stat(path, &file_stat);

    if(st < 0){
        st = -errno;
    }

    tosend_size += write_int_in_buffer(st, tosend_buffer+tosend_size);

    if(st >= 0){
        tosend_size += write_int_in_buffer((int)file_stat.st_mode, tosend_buffer+tosend_size);
        tosend_size += write_int_in_buffer((int)file_stat.st_nlink, tosend_buffer+tosend_size);
        tosend_size += write_int_in_buffer((int)file_stat.st_uid, tosend_buffer+tosend_size);
        tosend_size += write_int_in_buffer((int)file_stat.st_gid, tosend_buffer+tosend_size);
        tosend_size += write_int_in_buffer((int)file_stat.st_rdev, tosend_buffer+tosend_size);
        tosend_size += write_int_in_buffer((int)file_stat.st_size, tosend_buffer+tosend_size);
        tosend_size += write_int_in_buffer((int)file_stat.st_blocks, tosend_buffer+tosend_size);
    }

    send_data(sockfd, tosend_buffer, tosend_size);
    printf("end of getattr");
    return 0;
}


int serve_rmdir(int sockfd, char* path){
    char tosend_buffer[sizeof(uint32_t)];

    int res = rmdir(path);
    if (res == -1) res = -errno;
    int tosend_size = 0;
    tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
    send_data(sockfd, tosend_buffer, tosend_size);
    return 0;
}

int serve_mkdir(int sockfd, char* path, int mode){

    char tosend_buffer[sizeof(uint32_t)];

    int res = mkdir(path, (mode_t)mode);
    if (res == -1) res = -errno;
    int tosend_size = 0;
    tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
    send_data(sockfd, tosend_buffer, tosend_size);

    return 0;
}



int serve_readdir(int sockfd, char* path){
    printf("read dir\n");
    fflush(stdout);
    char tosend_buffer[1000];

    int tosend_size = 0;

    DIR *dp;
    struct dirent *de;
    // (void) offset;
    // (void) fi;
    int count = 0;

    dp = opendir(path);
    if (dp == NULL){
        tosend_size += write_int_in_buffer(-errno, tosend_buffer+tosend_size);
    }
    else{
        tosend_size += write_int_in_buffer(0, tosend_buffer+tosend_size);
        fflush(stdout);

        while ((de = readdir(dp)) != NULL) {
          
            tosend_size += write_int_in_buffer(de->d_ino, tosend_buffer+tosend_size);

            tosend_size += write_int_in_buffer(de->d_type << 12, tosend_buffer+tosend_size);


            tosend_size += write_int_in_buffer(strlen(de->d_name), tosend_buffer+tosend_size);
            tosend_size += write_string_in_buffer(de->d_name, tosend_buffer+tosend_size);
            count++;
        }
    }

    char tosend_final_buffer[1000];
    int tosend_final_size = 0;
    tosend_final_size += write_int_in_buffer(count, tosend_final_buffer+tosend_final_size);

    memcpy(tosend_final_buffer+tosend_final_size, tosend_buffer, tosend_size);
    tosend_final_size += tosend_size;


    closedir(dp);

    send_data(sockfd, tosend_final_buffer, tosend_final_size);  
    return 0;

}



int serve_open(int sockfd, char* path, int flags, int mode){

    char tosend_buffer[1000];
    int tosend_size = 0;

    int res;

    if(mode != 0){
        res = open(path, flags, (mode_t)mode);
    }else{
        res = open(path, flags);
    }

    if (res == -1)
        res = -errno;

    tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);

    send_data(sockfd, tosend_buffer, tosend_size);    
    printf("sent data from open\n");

    return 0;
}




// int serve_read(int sockfd, int fd, int size, int offset){

//     char tosend_buffer [50];

//     int tosend_size = 0;
//     char buffer[size];
//     int res = pread(fd, buffer, size, offset);

//     if (res < 0)
//         tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
//     else{
//         tosend_size += write_int_in_buffer(0, tosend_buffer+tosend_size);
//         tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);

//         strcpy(tosend_buffer + tosend_size, buffer);
//         tosend_size += res;
//     }
//     tosend_buffer[tosend_size] = '\0';
//     send_data(sockfd, tosend_buffer, tosend_size);  
//     return 0;

// }



int serve_read(int sockfd, int fd, int size, int offset){

    char tosend_buffer [size+50];

    int tosend_size = 0;
    char buffer[size];
    int res = pread(fd, buffer, size, offset);

    if (res < 0)
        tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
    else{
        tosend_size += write_int_in_buffer(0, tosend_buffer+tosend_size);
        tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);

        strcpy(tosend_buffer + tosend_size, buffer);
        tosend_size += res;
    }
    tosend_buffer[tosend_size] = '\0';
    printf("\n\nread tosend buffer %s\n\n\n", tosend_buffer);
    send_data(sockfd, tosend_buffer, tosend_size);  
    return 0;

}




int serve_unlink(int sockfd, char* path){
    char tosend_buffer[sizeof(uint32_t)];
    int tosend_size = 0;
    int res = unlink(path);
    if (res == -1) res  = -errno;
    tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
    send_data(sockfd, tosend_buffer, tosend_size);   
    return 0;
}


int serve_release(int sockfd, int fd){
    int res = close(fd);
    printf("release serve %d \n", res);
    if (res == -1) res  = -errno;
    char tosend_buffer[sizeof(uint32_t)];
    int tosend_size = 0;
    tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
    send_data(sockfd, tosend_buffer, tosend_size); 
    printf("endof serve\n");  
    return 0;
}


int serve_write(int sockfd, char* buf, int size, int offset, int fd){ 
    printf("write serve\n");
    int res = pwrite(fd, buf, size, offset);
    if (res == -1) res = -errno;
    char tosend_buffer[sizeof(uint32_t)];
    int tosend_size = 0;
    tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
    send_data(sockfd, tosend_buffer, tosend_size); 
    printf("endof write\n"); 
    return 0;
}




int receive_data(int sockfd){
    while(1){
        printf("go \n");
        char* recvbuffer = malloc(2000);
        char* initialbuffer = recvbuffer;
        // int recieved = read(sockfd, recvbuffer, 2000);
        read(sockfd, recvbuffer, 2000);

        int type = read_int_from_buffer(&recvbuffer);
        // if(type < 0) continue;
        printf("type %d\n", type);
        // if(type < 0) continue;

        int len = read_int_from_buffer(&recvbuffer);
        printf("len %d\n", len);
        char path[1024];
        read_string_from_buffer(&recvbuffer, len, path);
        path[len] = '\0';

        char finalpath[1024];
        memcpy(finalpath, serverpath, strlen(serverpath)+1);
        strcat(finalpath, path);
        printf("path %s\n", finalpath);
        if(type == GETATTR){
            serve_getattr(sockfd, finalpath);
        }
        else if(type == READDIR){
            serve_readdir(sockfd, finalpath);
        }
        else if(type == OPEN){
            int flags = read_int_from_buffer(&recvbuffer);
            serve_open(sockfd, finalpath, flags, 0);
        }
        else if(type == READ){
            int fd = read_int_from_buffer(&recvbuffer);
            int size = read_int_from_buffer(&recvbuffer);
            int offset = read_int_from_buffer(&recvbuffer);
            serve_read(sockfd, fd, size, offset);
        }
        else if(type == MKDIR){
            int mode = read_int_from_buffer(&recvbuffer);
            serve_mkdir(sockfd, finalpath, mode);
        }
        else if(type == RMDIR){
            serve_rmdir(sockfd, finalpath);
        }
        else if(type == CREATE){
            int flags = read_int_from_buffer(&recvbuffer);
            int mode = read_int_from_buffer(&recvbuffer);
            serve_open(sockfd, finalpath, flags, mode);
        }
        else if(type == UNLINK){
            serve_unlink(sockfd, finalpath);
        }
        else if(type == RELEASE){
            int fd = read_int_from_buffer(&recvbuffer);
            serve_release(sockfd, fd);
        } 
        else if(type == WRITE){
            int fd = read_int_from_buffer(&recvbuffer);
            int offset = read_int_from_buffer(&recvbuffer);
            int size = read_int_from_buffer(&recvbuffer);
            char buf[size];
            read_string_from_buffer(&recvbuffer, size, buf);
            serve_write(sockfd, buf, size, offset, fd);
        } 
        else if(type == TRUNCATE){
            int size = read_int_from_buffer(&recvbuffer);
            int res = truncate(finalpath, size);
            char tosend_buffer[sizeof(uint32_t)];
            if(res < 0) res = -errno;
            int tosend_size = 0;
            tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
            send_data(sockfd, tosend_buffer, tosend_size); 
        } 
        free(initialbuffer);
    }
    
    return 0;
}


// int receive_data(int sockfd){
//     while(1){
//         printf("go \n");

//         int type = read_int_from_socket(sockfd);
//         // if(type < 0) continue;
//         printf("type %d\n", type);
//         // if(type < 0) continue;

//         int len = read_int_from_socket(sockfd);
//         printf("len %d\n", len);
//         char path[1024];
//         read_string_from_socket(sockfd, len, path);
//         path[len] = '\0';

//         char finalpath[1024];
//         memcpy(finalpath, serverpath, strlen(serverpath)+1);
//         strcat(finalpath, path);
//         printf("path %s\n", finalpath);
//         if(type == GETATTR){
//             serve_getattr(sockfd, finalpath);
//         }
//         else if(type == READDIR){
//             serve_readdir(sockfd, finalpath);
//         }
//         else if(type == OPEN){
//             int flags = read_int_from_socket(sockfd);
//             serve_open(sockfd, finalpath, flags, 0);
//         }
//         else if(type == READ){
//             int fd = read_int_from_socket(sockfd);
//             int size = read_int_from_socket(sockfd);
//             int offset = read_int_from_socket(sockfd);
//             serve_read(sockfd, fd, size, offset);
//         }
//         else if(type == MKDIR){
//             int mode = read_int_from_socket(sockfd);
//             serve_mkdir(sockfd, finalpath, mode);
//         }
//         else if(type == RMDIR){
//             serve_rmdir(sockfd, finalpath);
//         }
//         else if(type == CREATE){
//             int flags = read_int_from_socket(sockfd);
//             int mode = read_int_from_socket(sockfd);
//             serve_open(sockfd, finalpath, flags, mode);
//         }
//         else if(type == UNLINK){
//             serve_unlink(sockfd, finalpath);
//         }
//         else if(type == RELEASE){
//             int fd = read_int_from_socket(sockfd);
//             serve_release(sockfd, fd);
//         } 
//         else if(type == WRITE){
//             int fd = read_int_from_socket(sockfd);
//             int offset = read_int_from_socket(sockfd);
//             int size = read_int_from_socket(sockfd);
//             char buf[size];
//             read_string_from_socket(sockfd, size, buf);
//             serve_write(sockfd, buf, size, offset, fd);
//         } 
//         else if(type == TRUNCATE){
//             int size = read_int_from_socket(sockfd);
//             int res = truncate(finalpath, size);
//             char tosend_buffer[sizeof(uint32_t)];
//             if(res < 0) res = -errno;
//             int tosend_size = 0;
//             tosend_size += write_int_in_buffer(res, tosend_buffer+tosend_size);
//             send_data(sockfd, tosend_buffer, tosend_size); 
//         } 
//     }
    
//     return 0;
// }


int start_socket(int listenfd){
    int sockfd = 0;  
    while(1)
    {
        sockfd = accept(listenfd, (struct sockaddr*)NULL, NULL); 

        receive_data(sockfd);       
            
        close(sockfd);
        sleep(1);
    }
    return 1;
}


// DIR *dir;
// struct dirent *dp;
// char * file_name;
// while ((dp=readdir(dir)) != NULL) {

//         file_name = dp->d_name;            
// }


int main(int argc, char *argv[])
{
    if (getcwd(serverpath, sizeof(serverpath)) != NULL){
        strcat(serverpath, argv[3]);


        int sockfd = get_listener_socket(strtol(argv[2], (char **)NULL, 10));
        start_socket(sockfd);
        return 0;

    }
    return 1;
}