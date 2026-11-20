#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/syscall.h>
#include <unistd.h>

#if !defined(__x86_64__) || defined(__ILP32__)
#error Native Linux x86_64 is required; there is no unsandboxed fallback.
#endif
#define ABI "mediastorm-local-probe/1"
#define MAX_INPUT_SIZE (8LL << 30)
#define DENY(n) BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,(n),0,1),BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_ERRNO|EPERM)

static int filter(void) {
    struct sock_filter code[] = {
        BPF_STMT(BPF_LD|BPF_W|BPF_ABS,offsetof(struct seccomp_data,arch)),
        BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K,AUDIT_ARCH_X86_64,1,0),
        BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_KILL_PROCESS),
        BPF_STMT(BPF_LD|BPF_W|BPF_ABS,offsetof(struct seccomp_data,nr)),
        BPF_JUMP(BPF_JMP|BPF_JSET|BPF_K,0x40000000U,0,1),
        BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_ERRNO|EPERM),
        DENY(__NR_socket),DENY(__NR_socketpair),DENY(__NR_connect),
        DENY(__NR_bind),DENY(__NR_listen),DENY(__NR_accept),DENY(__NR_accept4),
        DENY(__NR_sendto),DENY(__NR_sendmsg),DENY(__NR_sendmmsg),
        DENY(__NR_recvfrom),DENY(__NR_recvmsg),DENY(__NR_recvmmsg),
        DENY(__NR_io_uring_setup),DENY(__NR_io_uring_enter),DENY(__NR_io_uring_register),
        DENY(__NR_ptrace),DENY(__NR_process_vm_readv),DENY(__NR_process_vm_writev),DENY(__NR_pidfd_getfd),
        BPF_STMT(BPF_RET|BPF_K,SECCOMP_RET_ALLOW)
    };
    struct sock_fprog program={.len=sizeof(code)/sizeof(code[0]),.filter=code};
    return prctl(PR_SET_NO_NEW_PRIVS,1,0,0,0)||prctl(PR_SET_SECCOMP,SECCOMP_MODE_FILTER,&program);
}
static int regular(int fd, int executable) {
    struct stat st;
    unsigned char elf[5];
    if(fstat(fd,&st)||!S_ISREG(st.st_mode)||st.st_nlink!=1||st.st_size<=0||(fcntl(fd,F_GETFL)&O_ACCMODE)!=O_RDONLY)return -1;
    if(executable&&(pread(fd,elf,sizeof(elf),0)!=(ssize_t)sizeof(elf)||memcmp(elf,"\177ELF\2",5)))return -1;
    return 0;
}
static int inventory(void) {
    struct stat in,zero,out,err,ready,ack;
    if(regular(3,0)||fstat(3,&in)||in.st_size>MAX_INPUT_SIZE||lseek(3,0,SEEK_SET)<0)return -1;
    if(regular(4,1)||regular(5,1))return -1;
    if(fstat(0,&zero)||!S_ISCHR(zero.st_mode)||zero.st_rdev!=makedev(1,3))return -1;
    if(fstat(1,&out)||!S_ISFIFO(out.st_mode)||fstat(2,&err)||!S_ISFIFO(err.st_mode))return -1;
    if(fstat(6,&ready)||!S_ISFIFO(ready.st_mode)||fstat(7,&ack)||!S_ISFIFO(ack.st_mode))return -1;
    if(syscall(__NR_close_range,8U,~0U,0U))return -1;
    for(int fd=4;fd<=7;fd++)if(fcntl(fd,F_SETFD,FD_CLOEXEC))return -1;
    return 0;
}
static int denied(long n,long a,long b,long c) {
    errno=0;long result=syscall(n,a,b,c,0,0,0);
    if(result>=0&&n==__NR_socket)close((int)result);
    return result==-1&&errno==EPERM;
}
static int verify(void) {
    int n=0;
    n+=denied(__NR_socket,AF_INET,SOCK_STREAM,0);n+=denied(__NR_socket,AF_INET6,SOCK_STREAM,0);
    n+=denied(__NR_socket,AF_UNIX,SOCK_STREAM,0);n+=denied(__NR_socketpair,AF_UNIX,SOCK_STREAM,0);
    n+=denied(__NR_connect,3,0,0);n+=denied(__NR_io_uring_setup,1,0,0);
    n+=denied(__NR_io_uring_enter,-1,0,0);n+=denied(__NR_io_uring_register,-1,0,0);
    n+=denied(__NR_ptrace,0,0,0);n+=denied(__NR_pidfd_getfd,-1,0,0);
    n+=denied(__NR_getpid|0x40000000U,0,0,0);
    printf("{\"abi\":\"%s\",\"deniedChecks\":%d,\"expected\":11,\"filter\":%d}\n",ABI,n,prctl(PR_GET_SECCOMP));
    return n==11?0:79;
}
int main(int argc,char **argv) {
    int check=argc==2&&!strcmp(argv[1],"--verify");
    if(!check&&(argc!=3||strcmp(argv[1],"--probe")||(strcmp(argv[2],"mpegts")&&strcmp(argv[2],"matroska")&&strcmp(argv[2],"mov"))))return 64;
    pid_t parent=getppid();
    if(parent==1||prctl(PR_SET_PDEATHSIG,SIGKILL)||getppid()!=parent||setpgid(0,0))return 71;
    if(inventory())return 72;
    struct rlimit cpu={12,12},core={0,0},memory={1024ULL<<20,1024ULL<<20},files={64,64};
    if(setrlimit(RLIMIT_CPU,&cpu)||setrlimit(RLIMIT_CORE,&core)||setrlimit(RLIMIT_AS,&memory)||setrlimit(RLIMIT_NOFILE,&files))return 73;
    if(filter()||prctl(PR_GET_NO_NEW_PRIVS,0,0,0,0)!=1||prctl(PR_GET_SECCOMP)!=2)return 74;
    if(dprintf(6,"%s %ld\n",ABI,(long)getpid())<=0)return 75;
    unsigned char ack=0;
    if(read(7,&ack,1)!=1||ack!='S')return 76;
    close(6);close(7);
    if(check){close(4);close(5);return verify();}
    char *args[40];int n=0;
    args[n++]="ffprobe";args[n++]="-v";args[n++]="error";args[n++]="-threads";args[n++]="1";
    args[n++]="-probesize";args[n++]="32768";args[n++]="-analyzeduration";args[n++]="100000";
    args[n++]="-protocol_whitelist";args[n++]="fd";args[n++]="-format_whitelist";args[n++]=argv[2];
    args[n++]="-f";args[n++]=argv[2];args[n++]="-fd";args[n++]="3";args[n++]="-blocksize";args[n++]="32768";
    if(!strcmp(argv[2],"mov")){args[n++]="-enable_drefs";args[n++]="0";args[n++]="-use_absolute_path";args[n++]="0";}
    args[n++]="-i";args[n++]="fd:";
    args[n++]="-show_entries";args[n++]="stream=index,codec_type,codec_name,width,height,channels,sample_rate,avg_frame_rate:stream_tags=language:stream_disposition=default,forced:format=duration";
    args[n++]="-of";args[n++]="json";args[n]=NULL;
    char *environment[]={"LANG=C","LC_ALL=C","TZ=UTC",NULL};
    fexecve(5,args,environment);
    return 77;
}
