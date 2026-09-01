#define _DEFAULT_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
#include <alsa/asoundlib.h>
#include <libavutil/log.h>
#include <libavutil/fifo.h>
#include <libavutil/cpu.h>
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
#include <libswresample/swresample.h>
#include <libavutil/time.h>
#include <libswscale/swscale.h>
#include "lvgl.h"
#include "gui_guider.h"

extern gg_ui_t guider_ui;   // GUI Guider 的全局 UI 实例（main.c 里定义）

#define VIDEO_FRAME_QUEUE_SIZE 16
#define AUDIO_FRAME_QUEUE_SIZE 16
#define MAX_QUEUE_SIZE         (15 * 1024 * 1024)
#define IMG_W                  640
#define IMG_H                  384

static uint8_t img_rgb[IMG_W * IMG_H *2];
static lv_image_dsc_t img_dsc;
static lv_obj_t *g_image=NULL;

static pthread_cond_t continue_read_cond;

/* 切视频请求：事件回调里只挂起，真正的停/起在 main 主循环无锁区执行（video_process_switch） */
static int  g_switch_pending = 0;
static char *g_switch_filename = NULL;

/* 默认音量：默认值 1，初始 = 1 * 0.35 = 0.35（35%）。切换视频后沿用该值 */
static float g_volume = 0.35f;


typedef struct PacketQueue{
    AVFifo              *pkts;
    int                 nb_packets;
    int                 size;
    int                 serial;
    int                 abort_request;  
    int                 eof;            
    int64_t             duration;

    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
}PacketQueue;

typedef struct VideoFrame{
    AVFrame             *frame;
    int                 width;
    int                 height;
    int                 format;
    int                 serial;

    AVRational          sample_aspect_ratio;

    double              pts;
    double              duration;
    int64_t             pos;
}VideoFrame;

typedef struct{
    VideoFrame          queue[VIDEO_FRAME_QUEUE_SIZE];
    int                 rindex;
    int                 windex;
    int                 size;
    int                 max_size;
    int                 abort_request;
    int                 eof;

    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
}VideoFrameQueue;

typedef struct AudioFrame{
    uint8_t             *data;
    int                 size;
    int                 serial;
    double              pts;
    double              duration; 
}AudioFrame;

typedef struct AudioFrameQueue{
    AudioFrame          queue[AUDIO_FRAME_QUEUE_SIZE];
    int                 rindex;
    int                 windex;
    int                 size;
    int                 max_size;
    int                 abort_request;
    int                 eof;
    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
}AudioFrameQueue;

typedef struct Clock{
    double              pts;
    double              last_updated;
    double              pts_drift;   
    int                 pause;         
}Clock;

enum video_change{
    VIDEO_LAST,
    VIDEO_NEXT,
};

enum{
    AV_SYNC_AUDIO_MASTER,
    AV_SYNC_VIDEO_MASTER,
};

typedef struct VideoContainer{
    AVFormatContext     *ifmtctx;
    int                 paused;
    int                 quit;
    int                 seek_req;
    int                 read_eof;
    char                *filename;
    pthread_mutex_t     codec_mutex;
    int64_t             seek_pos;
    pthread_t           read_tid;

    //视频相关内容
    AVCodecContext      *Vctx;
    AVPacket            *Vpkt;
    AVFrame             *Vframe;
    AVStream            *Vstream;
    PacketQueue         video_pq;
    VideoFrameQueue     video_fq;
    int                 video_index;
    pthread_t           video_decode_tid;
    pthread_t           video_play_tid;
    Clock               vidclk;
    double              video_clock;
    int                 video_eof;

    //音频相关内容
    AVCodecContext      *Actx;
    AVPacket            *Apkt;
    AVFrame             *Aframe;
    AVStream            *Astream;
    SwrContext          *swr_ctx;
    PacketQueue         audio_pq;
    AudioFrameQueue     audio_fq;
    int                 audio_index;
    int                 audio_eof;
    int                 cur_audio_offset;
    float               volume;          /* 软件音量增益 0.0~1.0 */
    pthread_t           audio_decode_tid;
    pthread_t           audio_play_tid;
    Clock               audclk;
    double              audio_clock;
    double              last_seek_time;   /* 最近一次 seek 的系统时间（秒），用于跳过 seek 后的误丢帧 */
    int                 queues_ready;   /* init 里队列/互斥/条件变量全部就绪后置 1，deinit 据此安全释放 */
}VideoContainer;

typedef struct MyPacketElement{
    AVPacket *pkt;
    int serial;
}MyPacketElement;

typedef struct file_node{
    char *filename;
    struct file_node *next;
}file_node;

static VideoContainer* VideoContainer_init(char *filename);
static void VideoContainer_deinit(VideoContainer *vc);
static void player_shutdown(VideoContainer *vc);

void video_switch_to_file(const char *path);   /* music_list 点击：切到指定文件 */
void video_seek_to(double seconds);            /* 进度条拖动：绝对 seek */
void video_player_exit(void);                  /* 退出按钮点击（持锁）：置请求标志 */
void video_player_process_exit(void);          /* 主循环无锁区：真正停线程、释放资源 */

static char *get_file_last_name(char *path)
{
    if(path==NULL||*path=='\0'){
        return "";
    }

    char *p=path+strlen(path)-1;
    while(p>path){
        if(*p=='/') return p+1;
        p--;
    }

    return path;
}

file_node *create_file_node(char *filename)
{
    file_node *temp=(file_node *)malloc(sizeof(file_node));
    if(!temp){
        perror("malloc fail");
        return NULL;
    }

    temp->filename=av_strdup(filename);
    temp->next=NULL;
    return temp;
}

static file_node *head=NULL;
static file_node *head_now=NULL;

static void list_free(file_node *head)
{
    file_node *p=head;
    while(p!=NULL){
        file_node *temp=p;
        p=p->next;
        free(temp);
    }
    head=NULL;
}

static void inserttail(file_node *head,char *filename)
{
    file_node *temp=create_file_node(filename);
    file_node *p=head;

    while(p->next!=NULL){
        p=p->next;
    }

    p->next=temp;
}

/* ---------- 播放列表：从当前视频所在目录扫描，供 上一部/下一部 切换 ---------- */
static int is_video_file(const char *name)
{
    static const char *exts[] = {".mp4",".avi",".mkv",".ts",".mov",".flv",".rmvb",
                                 ".wmv",".m4v",".3gp",".mpg",".mpeg",NULL};
    const char *dot = strrchr(name, '.');
    if(!dot) return 0;
    for(int i=0; exts[i]; i++){
        if(!strcasecmp(dot, exts[i])) return 1;
    }
    return 0;
}

static char *get_parent_dir(const char *path)
{
    const char *p = strrchr(path, '/');
    if(!p) return av_strdup(".");
    if(p == path) return av_strdup("/");
    char *dir = av_malloc(p - path + 1);
    if(!dir) return NULL;
    memcpy(dir, path, p - path);
    dir[p - path] = 0;
    return dir;
}

static void playlist_free(void)
{
    file_node *p = head;
    while(p){
        file_node *t = p;
        p = p->next;
        av_free(t->filename);
        free(t);
    }
    head = NULL;
    head_now = NULL;
}

/* 扫描 dir 下的视频文件建立播放列表；返回文件数 */
static int playlist_build(const char *dir)
{
    playlist_free();

    /* 用 POSIX opendir 扫本地目录（avio_open_dir 在板子上不可靠，会静默返回 0 个文件） */
    DIR *d = opendir(dir);
    if(!d){
        av_log(NULL, AV_LOG_ERROR, "[playlist] open dir %s failed\n", dir);
        return 0;
    }

    int n = 0;
    struct dirent *de;
    struct stat st;
    while((de = readdir(d)) != NULL){
        if(de->d_name[0] == '.') continue;
        if(!is_video_file(de->d_name)) continue;

        char *fullpath = av_malloc(strlen(dir) + strlen(de->d_name) + 2);
        if(!fullpath) continue;
        sprintf(fullpath, "%s/%s", dir, de->d_name);

        /* FAT32 的 d_type 常为 DT_UNKNOWN，用 stat 确认是普通文件而非子目录 */
        if(stat(fullpath, &st) != 0 || !S_ISREG(st.st_mode)){
            av_free(fullpath);
            continue;
        }

        if(!head){ head = create_file_node(fullpath); head_now = head; }
        else inserttail(head, fullpath);
        av_free(fullpath);
        n++;
    }
    closedir(d);
    return n;
}

/* 让 head_now 指向与当前播放文件匹配的节点 */
static file_node *playlist_find(const char *filename)
{
    file_node *p = head;
    while(p){
        if(!strcmp(p->filename, filename)){
            head_now = p;
            return p;
        }
        p = p->next;
    }
    return NULL;
}

static char *select_next_file(file_node *node,int check_flag)
{
    if(node==NULL) return NULL;
    if(check_flag==VIDEO_NEXT){
        node=node->next;
        if(node==NULL) node=head;
        while(node!=NULL&&strcmp(node->filename,"")==0){
            node=node->next;
            if(node==NULL) node=head;
        }
    }else if(check_flag==VIDEO_LAST){
        file_node *p=head;
        file_node *prev=NULL;
        while(p!=node&&p!=NULL){
            prev=p;
            p=p->next;
        }
        if(prev!=NULL){
            node=prev;
        }else{
            node=head;
            while(node->next!=NULL) node=node->next;
        }
        while(node!=NULL&&strcmp(node->filename,"")==0){
            file_node *q=head,*qq=NULL;
            while(q!=NULL&&q!=node){
                qq=q;
                q=q->next;
            }
            node=(qq!=NULL)?qq:head;
        }
    }

    head_now=node;
    return node?node->filename:NULL;
}

static double get_clock(Clock *c)//得到pts值
{
    if(c->pause)
        return c->pts;
    else{
        double time=av_gettime_relative()/1000000.0;
        return c->pts_drift+time;
    }
}

static void set_clock_at(Clock *c,double pts,double systime)//设置时钟在固定位置
{
    c->pts=pts;
    c->last_updated=systime;
    c->pts_drift=c->pts-systime;
}

static void set_clock(Clock *c,double pts)//简化版设置时钟
{
    double time=av_gettime_relative()/1000000.0;
    set_clock_at(c,pts,time);
}

static void clock_init(Clock *c)
{
    c->pause=0;
    set_clock(c,NAN);
}

static int get_master_sync_type(VideoContainer *vc)
{
    if (vc->Astream)
        return AV_SYNC_AUDIO_MASTER;
    return AV_SYNC_VIDEO_MASTER;   /* 无音频流时回退视频主时钟 */
}

static double get_master_clock(VideoContainer *vc)
{
    double val = NAN;

    switch (get_master_sync_type(vc)) {
        case AV_SYNC_VIDEO_MASTER:
            val = get_clock(&vc->vidclk);
            break;
        case AV_SYNC_AUDIO_MASTER:
            val = get_clock(&vc->audclk);
            break;
        default:
            break;
    }
    return val;
}

static void stream_paused(VideoContainer *vc)
{
    set_clock(&vc->audclk, get_clock(&vc->audclk));
    set_clock(&vc->vidclk, get_clock(&vc->vidclk));

    vc->audclk.pause=1;
    vc->vidclk.pause=1;

    vc->paused=1;
}

static void stream_resume(VideoContainer *vc)
{
    vc->audclk.pause=0;
    vc->vidclk.pause=0;

    set_clock(&vc->audclk,vc->audclk.pts);
    set_clock(&vc->vidclk,vc->vidclk.pts);

    vc->paused=0;
}

static int packet_queue_init(PacketQueue *q)
{
    memset(q,0,sizeof(PacketQueue));//清除q中的内容
    q->pkts=av_fifo_alloc2(1,sizeof(MyPacketElement),AV_FIFO_FLAG_AUTO_GROW);

    if(!q->pkts) return AVERROR(ENOMEM);
    
    pthread_mutex_init(&q->mutex,NULL);

    pthread_cond_init(&q->cond,NULL);

    q->abort_request=1;
    return 0;
}

static void packet_queue_start(PacketQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    q->abort_request=0;
    pthread_mutex_unlock(&q->mutex);
}

static int packet_queue_put_priv(PacketQueue *q,AVPacket *pkt,int serial)
{
    MyPacketElement mypkt;
    int ret;
    
    if(q->abort_request)
        return -1;

    mypkt.pkt=pkt;
    mypkt.serial=serial;

    ret = av_fifo_write(q->pkts,&mypkt,1);
    if(ret<0) return ret;
    q->nb_packets++;
    q->size+=mypkt.pkt->size+sizeof(mypkt);
    q->duration+=mypkt.pkt->duration;
  
    pthread_cond_signal(&q->cond);

    return 0;
}

static void packet_queue_put(PacketQueue *q,AVPacket *pkt)
{
    AVPacket *pkt1;
    int ret;

    pkt1=av_packet_alloc();
    if(!pkt1) return;
    av_packet_move_ref(pkt1,pkt);
    pthread_mutex_lock(&q->mutex);

    ret = packet_queue_put_priv(q,pkt1,q->serial);
    if(ret<0)
        av_packet_free(&pkt1);

    pthread_mutex_unlock(&q->mutex);
}

static int packet_queue_get(PacketQueue *q,AVPacket *pkt,int waitflag,int *serial)
{
    MyPacketElement mypkt;
    int ret;

    pthread_mutex_lock(&q->mutex);

    while(1){
        if(q->abort_request){
            pthread_mutex_unlock(&q->mutex);   
            return -1;
        }
        if(q->eof&&q->size==0){
            pthread_mutex_unlock(&q->mutex);   
            return -1;
        }
        if(av_fifo_read(q->pkts,&mypkt,1)>=0){
            q->nb_packets--;
            q->size-=mypkt.pkt->size+sizeof(mypkt);
            q->duration-=mypkt.pkt->duration;
            av_packet_move_ref(pkt,mypkt.pkt);
            av_packet_free(&mypkt.pkt);
            if(serial) *serial=mypkt.serial;   
            ret=1;
            break;
        }else if(!waitflag){
            ret=0;
            break;
        }else{
            pthread_cond_wait(&q->cond,&q->mutex);
        }
    }
    pthread_mutex_unlock(&q->mutex);

    if(q->size < MAX_QUEUE_SIZE)   /* 队列字节数降到上限以下：唤醒可能因队列满而阻塞的读线程 */
        pthread_cond_signal(&continue_read_cond);

    return ret;
}

static void packet_queue_flush(PacketQueue *q)
{
    MyPacketElement mypkt;

    pthread_mutex_lock(&q->mutex);

    while(av_fifo_read(q->pkts,&mypkt,1)>0){
        av_packet_free(&mypkt.pkt);
    }
    q->duration=0;
    q->nb_packets=0;
    q->size=0;
    q->eof=0;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

static void packet_queue_destroy(PacketQueue *q)
{
    packet_queue_flush(q);
    av_fifo_freep2(&q->pkts);
    pthread_cond_destroy(&q->cond);
    pthread_mutex_destroy(&q->mutex);
}

static void packet_queue_abort(PacketQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    q->abort_request=1;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

static void packet_queue_read_finish(PacketQueue *q)
{    
    pthread_mutex_lock(&q->mutex);
    q->eof=1;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);

}

static int video_frame_queue_init(VideoFrameQueue *fq,int max_size)
{
    memset(fq,0,sizeof(VideoFrameQueue));//给fq中的所有元素置零了，包括rindex和windex

    pthread_cond_init(&fq->cond,NULL);
    pthread_mutex_init(&fq->mutex,NULL);
    fq->max_size=max_size;

    for(int i=0;i<fq->max_size;i++){
        fq->queue[i].frame=av_frame_alloc();
        if(!fq->queue[i].frame) return -1;
    }
    return 0;
}

static void video_frame_queue_flush(VideoFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    for(int i=0;i<fq->max_size;i++){
        av_frame_unref(fq->queue[i].frame);
        fq->queue[i].pts=NAN;
        fq->queue[i].duration=0;
        fq->queue[i].pos=0;
    }
    fq->rindex=0;
    fq->windex=0;
    fq->size=0;
    fq->eof=0;   /* seek 后要能重新入队，eof 必须清掉 */
    pthread_cond_signal(&fq->cond);
    pthread_mutex_unlock(&fq->mutex);
}

static void video_frame_queue_destory(VideoFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    for(int i=0;i<fq->max_size;i++){
        VideoFrame *f=&fq->queue[i];
        av_frame_unref(f->frame);
        av_frame_free(&f->frame);
    }
    pthread_mutex_unlock(&fq->mutex);
    pthread_mutex_destroy(&fq->mutex);
    pthread_cond_destroy(&fq->cond);
}

static void video_frame_queue_push(VideoFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    fq->windex++;
    if(fq->windex==fq->max_size) fq->windex=0;
    fq->size++;
    pthread_cond_signal(&fq->cond);
    pthread_mutex_unlock(&fq->mutex);
}

static void video_frame_queue_next(VideoFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    av_frame_unref(fq->queue[fq->rindex].frame);    
    fq->rindex++;
    if(fq->rindex==fq->max_size) fq->rindex=0;
    fq->size--;
    pthread_cond_signal(&fq->cond);
    pthread_mutex_unlock(&fq->mutex);   
}

static VideoFrame *video_frame_queue_read_pos(VideoFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    while(fq->size<=0&&!fq->abort_request&&!fq->eof){
        pthread_cond_wait(&fq->cond,&fq->mutex);
    }
    pthread_mutex_unlock(&fq->mutex);

    if(fq->abort_request) return NULL;
    if(fq->size<=0&&fq->eof) return NULL;   /* 播完：队列空且 EOF，让播放线程干净退出 */

    return &fq->queue[fq->rindex];
}

static VideoFrame *video_frame_queue_write_pos(VideoFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    while((fq->size>=fq->max_size)&&!(fq->abort_request)){
        pthread_cond_wait(&fq->cond,&fq->mutex);
    }
    pthread_mutex_unlock(&fq->mutex);

    if(fq->abort_request) return NULL;

    return &fq->queue[fq->windex];
}

static void video_frame_queue_write(VideoContainer *vc,
                                    AVFrame *frame,
                                    double pts,
                                    double duration,
                                    int64_t pos)
{
    VideoFrame *vp=NULL;

    vp=video_frame_queue_write_pos(&vc->video_fq);
    if(!vp) return;   /* 中止（切视频/退出）时写不进去，直接丢弃这一帧 */

    vp->sample_aspect_ratio=frame->sample_aspect_ratio;

    vp->width=frame->width;
    vp->height=frame->height;
    vp->format=frame->format;

    vp->pts=pts;
    vp->duration=duration;
    vp->pos=pos;
    vp->serial=vc->video_pq.serial;   // 记录这一帧属于哪一代

    av_frame_move_ref(vp->frame,frame);
    video_frame_queue_push(&vc->video_fq);
   
}

static int audio_frame_queue_init(AudioFrameQueue *fq,int max_size)
{
    memset(fq,0,sizeof(AudioFrameQueue));

    pthread_cond_init(&fq->cond,NULL);

    pthread_mutex_init(&fq->mutex,NULL);

    fq->max_size=max_size;

    for(int i=0;i<max_size;i++){
        fq->queue[i].data=NULL;
    }
    return 0;
}

static void audio_frame_queue_flush(AudioFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    for(int i=0;i<fq->max_size;i++){
        av_freep(&fq->queue[i].data);
        fq->queue[i].size = 0;
        fq->queue[i].pts = NAN;
        fq->queue[i].duration = 0;
    }
    fq->rindex = 0;
    fq->windex = 0;
    fq->size = 0;
    pthread_cond_signal(&fq->cond);
    pthread_mutex_unlock(&fq->mutex);
}

static void audio_frame_queue_destroy(AudioFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    for(int i=0;i<fq->size;i++){
        int idx = (fq->rindex + i) % fq->max_size;
        av_freep(&fq->queue[idx].data);
        fq->queue[idx].size = 0;
        fq->queue[idx].pts = 0;
        fq->queue[idx].duration = 0;
        fq->queue[idx].serial = 0;
    }
    pthread_mutex_unlock(&fq->mutex);
    pthread_cond_destroy(&fq->cond);
    pthread_mutex_destroy(&fq->mutex);
}

static int audio_frame_queue_write(AudioFrameQueue *fq,
                                        uint8_t *buf,
                                        int data_size,
                                        double pts,
                                        double duration,
                                        int serial)
{
    pthread_mutex_lock(&fq->mutex);

    while(fq->size >= fq->max_size && !fq->abort_request){
        pthread_cond_wait(&fq->cond,&fq->mutex);
    }

    if(fq->abort_request){
        pthread_mutex_unlock(&fq->mutex);
        return -1;
    }

    if(fq->queue[fq->windex].data)
        av_freep(&fq->queue[fq->windex].data);
    fq->queue[fq->windex].data=buf;
    fq->queue[fq->windex].pts=pts;
    fq->queue[fq->windex].duration=duration;
    fq->queue[fq->windex].size=data_size;
    fq->queue[fq->windex].serial=serial;   

    pthread_cond_signal(&fq->cond);
    pthread_mutex_unlock(&fq->mutex);
    return 0;
}

static void audio_frame_queue_push(AudioFrameQueue *fq,
                                    uint8_t *buf,
                                    int data_size,
                                    double pts,
                                    double duration,
                                    int serial)
{
    if(audio_frame_queue_write(fq,buf,data_size,pts,duration,serial)<0){
        av_free(buf);
        return;
    }

    pthread_mutex_lock(&fq->mutex);
    fq->windex++;
    if(fq->windex==fq->max_size) fq->windex=0;
    fq->size++;
    pthread_cond_signal(&fq->cond);
    pthread_mutex_unlock(&fq->mutex);
}

static void audio_frame_queue_next(AudioFrameQueue *fq)
{
    pthread_mutex_lock(&fq->mutex);
    av_freep(&fq->queue[fq->rindex].data);
    fq->queue[fq->rindex].size=0;
    fq->queue[fq->rindex].pts=0;
    fq->queue[fq->rindex].duration=0;
    fq->queue[fq->rindex].serial=0;
    fq->rindex++;
    if(fq->rindex==fq->max_size) fq->rindex=0;
    fq->size--;
    pthread_cond_signal(&fq->cond);
    pthread_mutex_unlock(&fq->mutex);    
}

static double synchronize_video(VideoContainer *vc,AVFrame *frame,double pts)
{
    double frame_delay;

    if(!isnan(pts)){
        vc->video_clock=pts;
    }else{
        pts=vc->video_clock;
    }

    frame_delay=av_q2d(vc->Vstream->time_base);

    frame_delay+=frame->repeat_pict*(frame_delay*0.5);
    vc->video_clock+=frame_delay;
    return pts;
}

static void *video_decode_thread(void *arg)
{
    VideoContainer *vc=(VideoContainer *)arg;
    int ret=-1;
    int pkt_serial;

    double pts;
    double duration;

    AVFrame *video_frame=NULL;
    VideoFrame *frame=NULL;

    AVRational tb=vc->Vstream->time_base;
    AVRational frame_rate=av_guess_frame_rate(vc->ifmtctx,vc->Vstream,NULL);

    video_frame=av_frame_alloc();

    for(;;){
        if(vc->quit){
            break;
        }

        // 阻塞等包（abort/eof 时返回 <=0 退出），不再 10ms 轮询刷屏
        if(packet_queue_get(&vc->video_pq,vc->Vpkt,1,&pkt_serial)<=0){
            break;
        }

        // 旧代包（seek 前入队的）直接丢弃
        if(pkt_serial != vc->video_pq.serial){
            av_packet_unref(vc->Vpkt);
            continue;
        }

        pthread_mutex_lock(&vc->codec_mutex);
        ret=avcodec_send_packet(vc->Vctx,vc->Vpkt);
        pthread_mutex_unlock(&vc->codec_mutex);
        if(ret<0){
            av_log(vc->Vctx,AV_LOG_ERROR,"failed to send pkt to video decoder!\n");
            goto _error;
        }

        while(ret>=0){
            pthread_mutex_lock(&vc->codec_mutex);
            ret=avcodec_receive_frame(vc->Vctx,video_frame);
            pthread_mutex_unlock(&vc->codec_mutex);
            if(ret == AVERROR(EAGAIN) || ret == AVERROR_EOF){
                break;
            }
            else if(ret<0){
                av_log(NULL,AV_LOG_ERROR,"Error during encoding\n");
                ret=-1;
                goto _error;
            } 

            duration=(frame_rate.num && frame_rate.den ? av_q2d((AVRational){frame_rate.den, frame_rate.num}) : 0);
            pts=(video_frame->pts == AV_NOPTS_VALUE) ? NAN : video_frame->pts * av_q2d(tb);
            pts=synchronize_video(vc,video_frame,pts);

            video_frame_queue_write(vc,video_frame,pts,duration,vc->Vpkt->pos);

            av_frame_unref(video_frame);
        }
    }
    ret=0;

_error:
    if(video_frame) av_frame_free(&video_frame);
    /* 通知播放线程：视频解码已结束（EOF/退出），队列排空后干净结束 */
    vc->video_eof = 1;
    pthread_mutex_lock(&vc->video_fq.mutex);
    vc->video_fq.eof = 1;
    pthread_cond_signal(&vc->video_fq.cond);
    pthread_mutex_unlock(&vc->video_fq.mutex);
    return NULL;
}

static void *video_play_thread(void *arg)
{
    VideoContainer *vc = (VideoContainer *)arg;
    VideoFrame *vp = NULL;
    struct SwsContext *sws = NULL;
    int src_w = 0, src_h = 0;
    int img_src_set = 0;

    while (!vc->quit) {
        if (vc->paused) {                 /* 暂停：卡住最后一帧，不消费队列 */
            usleep(10000);
            continue;
        }
        vp = video_frame_queue_read_pos(&vc->video_fq);   // 阻塞取帧（不加锁，避免拖住 LVGL）
        if (!vp) break;

        if (vp->serial != vc->video_pq.serial) {          // 旧代帧
            video_frame_queue_next(&vc->video_fq);
            continue;
        }

        // ★ 音频主时钟同步（复用 player.c 逻辑）
        if (!isnan(vp->pts) && vc->audio_clock > 0) {
            double diff = vp->pts - vc->audio_clock;
            if (diff > 0 && diff < 1.0) {
                /* 超前等音频：分片睡，暂停/退出能立刻响应，不用等完这一秒 */
                double left = diff;
                while (left > 0 && !vc->quit && !vc->paused) {
                    double slice = left > 0.02 ? 0.02 : left;
                    usleep((unsigned int)(slice * 1000000));
                    left -= slice;
                }
            } else if (diff < -0.5) {
                /* 持续落后 0.5s 才丢帧（渲染跟不上源帧率时兜底，保持音画同步）。
                   但 seek 后 1.5s 内不丢：视频从目标点之前的关键帧起播，
                   PTS 落后于音频目标是正常的追赶，此时丢帧会把画面冻住 */
                double now_s = av_gettime_relative() / 1000000.0;
                if (now_s - vc->last_seek_time > 1.5) {
                    video_frame_queue_next(&vc->video_fq);
                    continue;
                }
            }
        }

        // 第一次：按视频实际尺寸建 sws（在锁外建，sws_getContext 慢，不能占着 LVGL 锁）
        if (!sws) {
            src_w = vp->frame->width;
            src_h = vp->frame->height;
            sws = sws_getContext(
                src_w, src_h, (enum AVPixelFormat)vp->frame->format,
                IMG_W, IMG_H, AV_PIX_FMT_RGB565,
                SWS_FAST_BILINEAR, NULL, NULL, NULL);
            img_dsc.header.cf = LV_COLOR_FORMAT_RGB565;
            img_dsc.header.w  = IMG_W;
            img_dsc.header.h  = IMG_H;
            img_dsc.header.stride = IMG_W * 2;
            img_dsc.data_size = IMG_W * IMG_H * 2;
            img_dsc.data      = img_rgb;
        }

        // YUV → RGB565（写到 img_rgb，锁外做，和 LVGL 渲染并行）
        uint8_t *dst[] = { img_rgb, NULL, NULL, NULL };
        int dst_stride[] = { IMG_W * 2, 0, 0, 0 };
        sws_scale(sws,
                  vp->frame->data, vp->frame->linesize, 0, src_h,
                  dst, dst_stride);

        // LVGL 相关调用放锁内（与主循环 lv_timer_handler 互斥）
        lv_lock();
        if (!img_src_set) {
            lv_image_set_src(g_image, &img_dsc);
            img_src_set = 1;
        }
        lv_obj_invalidate(g_image);
        lv_unlock();

        video_frame_queue_next(&vc->video_fq);
    }

    if (sws) sws_freeContext(sws);
    return NULL;
}

static void *audio_decode_thread(void *arg)
{
    VideoContainer *vc=(VideoContainer *)arg;
    AVPacket pkt;
    int serial;

    while(!vc->quit){
        if (packet_queue_get(&vc->audio_pq, &pkt, 1, &serial) < 0)
            break;
        if (serial != vc->audio_pq.serial) {
            av_packet_unref(&pkt);
            continue;
        }

        pthread_mutex_lock(&vc->codec_mutex);
        int ret = avcodec_send_packet(vc->Actx, &pkt);
        pthread_mutex_unlock(&vc->codec_mutex);
        av_packet_unref(&pkt);

        if(ret<0){
            av_log(NULL,AV_LOG_ERROR,"fail to send packet to context\n");
            goto _fail;
        }      
        while(ret>=0){
            pthread_mutex_lock(&vc->codec_mutex);
            ret=avcodec_receive_frame(vc->Actx,vc->Aframe);
            pthread_mutex_unlock(&vc->codec_mutex);
            if(ret==AVERROR(EAGAIN)||ret==AVERROR_EOF){
                break;
            }else if(ret<0){
                av_log(NULL,AV_LOG_ERROR,"fail to receive frame from audio!\n");
                goto _fail;
            }

            uint8_t *out_planes[AV_NUM_DATA_POINTERS] = {0};
            int out_samples = av_rescale_rnd(
                vc->Aframe->nb_samples, 48000, vc->Actx->sample_rate, AV_ROUND_UP);
            int out_size = av_samples_get_buffer_size(NULL, 2, out_samples, AV_SAMPLE_FMT_S16, 0);
            out_planes[0] = av_malloc(out_size);
            if (!out_planes[0]) break;

            uint8_t *in_planes[AV_NUM_DATA_POINTERS] = {0};
            for (int i = 0; i < AV_NUM_DATA_POINTERS; i++)
                in_planes[i] = vc->Aframe->data[i];

            pthread_mutex_lock(&vc->codec_mutex);
            int got = swr_convert(vc->swr_ctx, out_planes, out_samples,
                                  (const uint8_t **)in_planes, vc->Aframe->nb_samples);
            pthread_mutex_unlock(&vc->codec_mutex);

            if(got < 0){   /* 转码失败：丢弃本帧，不能把负长度推给播放线程 */
                av_free(out_planes[0]);
                av_frame_unref(vc->Aframe);
                continue;
            }

            double pts = (vc->Aframe->pts != AV_NOPTS_VALUE)
                       ? vc->Aframe->pts * av_q2d(vc->Astream->time_base)
                       : NAN;
            double duration = (double)vc->Aframe->nb_samples / vc->Actx->sample_rate;  
            audio_frame_queue_push(&vc->audio_fq, out_planes[0], got*2*2, pts, duration,
                                   vc->audio_pq.serial);    
            av_frame_unref(vc->Aframe);     
        }
    }

_fail:
    /* 通知播放线程：音频解码已结束（EOF），队列排空后干净结束 */
    vc->audio_eof = 1;
    pthread_mutex_lock(&vc->audio_fq.mutex);
    vc->audio_fq.eof = 1;
    pthread_cond_signal(&vc->audio_fq.cond);
    pthread_mutex_unlock(&vc->audio_fq.mutex);
    return NULL;
}

static void *audio_play_thread(void *arg)
{
    VideoContainer *vc = (VideoContainer *)arg;
    snd_pcm_t *handle = NULL;
    snd_pcm_hw_params_t *params = NULL;
    int ret;

    // 板载 ALSA 库编译时默认配置路径是 PC 路径，必须指到板子上的配置
    if (access("/root/alsa-1.0.22/share/alsa/alsa.conf", F_OK) == 0)
        setenv("ALSA_CONFIG_PATH", "/root/alsa-1.0.22/share/alsa/alsa.conf", 1);

    if ((ret = snd_pcm_open(&handle, "hw:0,0", SND_PCM_STREAM_PLAYBACK, 0)) < 0) {
        fprintf(stderr, "snd_pcm_open: %s\n", snd_strerror(ret));
        return NULL;
    }
    snd_pcm_hw_params_malloc(&params);
    snd_pcm_hw_params_any(handle, params);
    snd_pcm_hw_params_set_access(handle, params, SND_PCM_ACCESS_RW_INTERLEAVED);
    snd_pcm_hw_params_set_format(handle, params, SND_PCM_FORMAT_S16_LE);
    snd_pcm_hw_params_set_channels(handle, params, 2);
    snd_pcm_hw_params_set_rate_near(handle, params, (unsigned int *)&(int){48000}, 0);
    if ((ret = snd_pcm_hw_params(handle, params)) < 0) {
        fprintf(stderr, "snd_pcm_hw_params: %s\n", snd_strerror(ret));
        goto _fail;
    }

    while (!vc->quit) {
        if (vc->paused) {                 /* 暂停：不写 ALSA、不推进 audio_clock */
            usleep(10000);
            continue;
        }
        uint8_t *frame_data = NULL;
        uint8_t buf[65536];   /* 足够装下最大 AAC 帧重采样后的数据（1024采样@44.1k→48k≈4460B，4096采样帧≈44KB），绝不能被截断 */
        int     n = 0;
        double   frame_pts  = 0;
        int      frame_serial = 0;

        pthread_mutex_lock(&vc->audio_fq.mutex);
        if(vc->audio_fq.size > 0){
            AudioFrame *cur = &vc->audio_fq.queue[vc->audio_fq.rindex];
            frame_data   = cur->data;
            n            = cur->size;
            if(n > (int)sizeof(buf)) n = sizeof(buf);   /* 超长截断 */
            else if(n < 0) n = 0;                        /* 损坏的负长度钳到 0，防巨型 memcpy */
            memcpy(buf, cur->data, n);
            frame_pts    = cur->pts;
            frame_serial = cur->serial;
            
        }
        pthread_mutex_unlock(&vc->audio_fq.mutex);

        if(!frame_data){
            if(vc->audio_eof)
                break;
            pthread_mutex_lock(&vc->audio_fq.mutex);
            pthread_cond_wait(&vc->audio_fq.cond,&vc->audio_fq.mutex);
            pthread_mutex_unlock(&vc->audio_fq.mutex);
            continue;
        }
                
        if(frame_serial != vc->audio_pq.serial){
            audio_frame_queue_next(&vc->audio_fq);
            vc->cur_audio_offset = 0;
            continue;
        }

        if(n <= 0){   /* 空/损坏帧：消费掉，不写声卡 */
            audio_frame_queue_next(&vc->audio_fq);
            vc->cur_audio_offset = 0;
            continue;
        }

        /* 软件音量：对 S16 样本按 vc->volume 增益（饱和钳位，避免削顶噪音） */
        if(vc->volume != 1.0f){
            int16_t *smp = (int16_t *)buf;
            int nsmp = n / 2;
            for(int i = 0; i < nsmp; i++){
                int32_t v = (int32_t)(smp[i] * vc->volume);
                if(v > 32767) v = 32767;
                else if(v < -32768) v = -32768;
                smp[i] = (int16_t)v;
            }
        }

        int frames = n / 4;
        int written = 0;
        while(written < frames){
            int w = snd_pcm_writei(handle, buf + written * 4, frames - written);
            if(w < 0){
                if(w == -EPIPE){
                    snd_pcm_prepare(handle); 
                    continue;
                }
                fprintf(stderr, "snd_pcm_writei: %s\n", snd_strerror(w));
                goto _fail;
            }
            written += w;
            vc->cur_audio_offset += 4 * w;
            double bytes_per_sample = 2.0 * vc->Actx->ch_layout.nb_channels;
            double played_sec = (double)vc->cur_audio_offset / (bytes_per_sample * vc->Actx->sample_rate);
            vc->audio_clock = frame_pts + played_sec;
            set_clock(&vc->audclk, vc->audio_clock);
        }
        audio_frame_queue_next(&vc->audio_fq);
        vc->cur_audio_offset = 0;
    }

_fail:
    snd_pcm_drain(handle);
    snd_pcm_close(handle);
    snd_pcm_hw_params_free(params);
    return NULL;
}

static int stream_component_open(VideoContainer *vc,int stream_index)
{
    AVStream *avstream=NULL;
    const AVCodec  *codec=NULL;
    AVCodecContext *avctx=NULL;
    AVFormatContext *Fotmatctx=vc->ifmtctx;
    int ret=-1;

    avstream=Fotmatctx->streams[stream_index];

    codec = avcodec_find_decoder(avstream->codecpar->codec_id);
    if(!codec){
        av_log(NULL,AV_LOG_ERROR,"don't find Codec\n");
        goto _error;
    }

    avctx = avcodec_alloc_context3(codec);
    if(!avctx){
        av_log(NULL,AV_LOG_ERROR,"No memory\n");
        goto _error;
    }

    ret=avcodec_parameters_to_context(avctx,avstream->codecpar);
    if(ret<0){
        av_log(NULL,AV_LOG_ERROR,"Can't open codec: %s\n",av_err2str(ret));  
        goto _error;    
    }

    // 视频：开启帧级多线程解码（H.264 单线程只占一个核，CPU 上不去）
    // 上限 4 线程：帧线程再往上开销变大、延迟变高，反而影响流畅度
    if (codec->type == AVMEDIA_TYPE_VIDEO) {
        avctx->thread_count = av_cpu_count();
        if (avctx->thread_count > 4) avctx->thread_count = 4;
        avctx->thread_type  = FF_THREAD_FRAME;
    }

    ret=avcodec_open2(avctx,codec,NULL);
    if(ret<0){
        av_log(NULL,AV_LOG_ERROR,"Can't open codec: %s\n",av_err2str(ret));
        goto _error;
    }
    switch(avctx->codec_type){
        case AVMEDIA_TYPE_AUDIO:
            pthread_create(&vc->audio_play_tid,NULL,audio_play_thread,vc);
            vc->cur_audio_offset=0;
            vc->audio_index=stream_index;
            vc->Astream=avstream;
            vc->Actx=avctx;
            vc->Aframe=av_frame_alloc();
            if(!vc->Aframe){
                av_log(NULL,AV_LOG_ERROR,"Failed to allocate memory to Aframe!\n");
                goto _error;
            }
            // 音频 swr：转 48000 S16 立体声（ALC5623 只支持 48k）
            AVChannelLayout out_layout = AV_CHANNEL_LAYOUT_STEREO;
            ret = swr_alloc_set_opts2(&vc->swr_ctx, &out_layout, AV_SAMPLE_FMT_S16, 48000,
                                      &vc->Actx->ch_layout, vc->Actx->sample_fmt,
                                      vc->Actx->sample_rate, 0, NULL);
            if(ret < 0 || !vc->swr_ctx){
                av_log(NULL,AV_LOG_ERROR,"Failed to create swr_ctx!\n");
                goto _error;
            }
            ret = swr_init(vc->swr_ctx);
            if(ret < 0){
                av_log(NULL,AV_LOG_ERROR,"Failed to init swr_ctx!\n");
                goto _error;
            }
            packet_queue_start(&vc->audio_pq);
            pthread_create(&vc->audio_decode_tid,NULL,audio_decode_thread,vc);
            break;
        case AVMEDIA_TYPE_VIDEO:
            vc->video_index=stream_index;
            vc->Vstream=avstream;
            vc->Vctx=avctx;
            vc->Vpkt=av_packet_alloc();
            if(!vc->Vpkt){
                av_log(NULL,AV_LOG_ERROR,"Failed to allocate memory to Vpkt!\n");
                goto _error;
            }

            packet_queue_start(&vc->video_pq);

            pthread_create(&vc->video_decode_tid,NULL,video_decode_thread,vc);
            pthread_create(&vc->video_play_tid,NULL,video_play_thread,vc);
            break;
        default:
            av_log(avctx,AV_LOG_ERROR,"Unkonw Codec type:%d\n",avctx->codec_type);
            break;
    }
    ret=0;
    goto _end;

_error:

    /* 若 avctx 已挂到 vc 上（vc->Actx/Vctx=avctx）中途才失败，先把 vc 里指针清掉，
       避免 deinit 再 avcodec_free_context 二次释放已 free 的 avctx */
    if(avctx){
        if(vc->Actx == avctx) vc->Actx = NULL;
        if(vc->Vctx == avctx) vc->Vctx = NULL;
        avcodec_free_context(&avctx);
    }

_end:
    return ret;
}

static void stream_seek(VideoContainer *vc,double increase)
{
    double pos=get_master_clock(vc);
    if(isnan(pos)) pos=0;
    pos+=increase;
    if(pos<0) pos=0;

    //不能超过文件时长（留 1 秒余量，避免 seek 到正好末尾导致立即 EOF 误判结束）
    double duration = 0;
    if(vc->ifmtctx && vc->ifmtctx->duration > 0){
        duration = (double)vc->ifmtctx->duration / AV_TIME_BASE;
    }
    double end_margin = 1.0;   // 末尾留白 1 秒
    if(duration > 0 && pos > duration - end_margin){
        pos = duration - end_margin;   // 钳制到末尾前 1 秒
    }else if(duration<=0){
        pos = 0;
    }

    vc->seek_pos=(uint64_t)(pos*AV_TIME_BASE);
    vc->seek_req=1;
}

static void do_seek(VideoContainer *vc)
{

    packet_queue_flush(&vc->video_pq);
    packet_queue_flush(&vc->audio_pq);
    video_frame_queue_flush(&vc->video_fq);
    audio_frame_queue_flush(&vc->audio_fq);

    // 翻代：递增 serial，旧数据全部作废
    pthread_mutex_lock(&vc->video_pq.mutex);
    vc->video_pq.serial++;
    pthread_mutex_unlock(&vc->video_pq.mutex);
    pthread_mutex_lock(&vc->audio_pq.mutex);
    vc->audio_pq.serial++;
    pthread_mutex_unlock(&vc->audio_pq.mutex);

    //清空解码器残留
    pthread_mutex_lock(&vc->codec_mutex);
    avcodec_flush_buffers(vc->Actx);
    avcodec_flush_buffers(vc->Vctx);
    swr_free(&vc->swr_ctx);
    vc->swr_ctx=NULL;
    /* 重建重采样器：free 后不重建，下一帧音频 swr_convert(NULL) 会段错误 */
    if(vc->Actx && vc->Actx->ch_layout.nb_channels > 0){
        AVChannelLayout out_layout = AV_CHANNEL_LAYOUT_STEREO;
        if(swr_alloc_set_opts2(&vc->swr_ctx, &out_layout, AV_SAMPLE_FMT_S16, 48000,
                               &vc->Actx->ch_layout, vc->Actx->sample_fmt,
                               vc->Actx->sample_rate, 0, NULL) >= 0 && vc->swr_ctx){
            swr_init(vc->swr_ctx);
        }
    }
    pthread_mutex_unlock(&vc->codec_mutex);

    //跳到目标位置（BACKWARD 保证落在关键帧，能正确解码）
    //音频流精确 seek（微秒 → 音频流 time_base）
    int64_t video_ts = av_rescale_q(vc->seek_pos,
                                    (AVRational){1, AV_TIME_BASE},
                                    vc->Vstream->time_base);
    avformat_seek_file(vc->ifmtctx, vc->video_index,
                       INT64_MIN, video_ts, INT64_MAX, AVSEEK_FLAG_BACKWARD);

    //再seek音频流（精确，放最后保证音频位置正确）
    int64_t audio_ts = av_rescale_q(vc->seek_pos,
                                    (AVRational){1, AV_TIME_BASE},
                                    vc->Astream->time_base);
    avformat_seek_file(vc->ifmtctx, vc->audio_index,
                       INT64_MIN, audio_ts, INT64_MAX, AVSEEK_FLAG_BACKWARD);

    //重置时钟到目标位置
    double target = vc->seek_pos / (double)AV_TIME_BASE;
    vc->audio_clock = target;
    vc->video_clock = target;
    set_clock(&vc->audclk, vc->audio_clock);
    set_clock(&vc->vidclk, vc->video_clock);

    vc->last_seek_time = av_gettime_relative() / 1000000.0;   /* 供播放线程跳过 seek 后的误丢帧 */
    vc->cur_audio_offset = 0;
    vc->audio_eof = 0;
    vc->read_eof = 0;
}

static void *read_thread(void *arg)
{
    VideoContainer *vc=(VideoContainer *)arg;
    AVFormatContext *Fmtctx=NULL;
    const AVCodec   *codec=NULL;
    AVPacket *pkt=NULL;
    pthread_mutex_t wait_mutex;
    int ret=-1;

    pthread_mutex_init(&wait_mutex,NULL);

    pkt=av_packet_alloc();
    if(!pkt){
        av_log(NULL,AV_LOG_ERROR,"Can't allocate memory for pkt!\n");
        goto _error;
    }

    ret = avformat_open_input(&Fmtctx,vc->filename,NULL,NULL);
    if(ret<0){
        av_log(NULL,AV_LOG_ERROR,"Fail to open media file!\n");
        goto _error;
    }

    vc->ifmtctx=Fmtctx;

    ret = avformat_find_stream_info(Fmtctx, NULL);
    if (ret < 0) {
        av_log(NULL,AV_LOG_ERROR,"Fail to open media file!\n");
        goto _error;
    }
    for(int i=0;i<Fmtctx->nb_streams;i++){
        if(Fmtctx->streams[i]->codecpar->codec_type==AVMEDIA_TYPE_VIDEO&&
            vc->video_index<0){
                vc->video_index=i;
            }
        if(Fmtctx->streams[i]->codecpar->codec_type==AVMEDIA_TYPE_AUDIO&&
            vc->audio_index<0){
                vc->audio_index=i;
            }
        if(vc->video_index>-1&&vc->audio_index>-1) break;        
    }
        
    if(vc->video_index==-1){
        av_log(NULL,AV_LOG_ERROR,"can't find video");
        goto _error;
    }

    if(vc->audio_index==-1){
        av_log(NULL,AV_LOG_ERROR,"can't find Audio");
        goto _error;
    }

    if(vc->audio_index>=0){
        stream_component_open(vc,vc->audio_index);
    }

    if(vc->video_index>=0){
        stream_component_open(vc,vc->video_index);
    }

    while(1){
        if(vc->quit){
            ret=-1;
            goto _error;
        }
        if(vc->seek_req){
            do_seek(vc);
            vc->seek_req=0;
        }
        if(vc->read_eof){
            usleep(10000);
            continue;
        }        
        /* 按 packet 队列的总字节数限流（AUTO_GROW 的 FIFO 无上限，不限会
           把整份文件的压缩包全读进内存 → 内存膨胀 → lowmemorykiller 杀进程）。
           之前拿帧数(0~16)跟 15MB 比恒为假，限流失效，已修正。 */
        if(vc->audio_pq.size + vc->video_pq.size > MAX_QUEUE_SIZE){
            pthread_mutex_lock(&wait_mutex);
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 10 * 1000000;   // 10ms，注意溢出归一化（否则非法 timespec 直接 EINVAL，退化成忙等）
            if(ts.tv_nsec >= 1000000000){
                ts.tv_sec++;
                ts.tv_nsec -= 1000000000;
            }
            pthread_cond_timedwait(&continue_read_cond,&wait_mutex,&ts);
            pthread_mutex_unlock(&wait_mutex);
        }

        ret=av_read_frame(Fmtctx,pkt);
        if(ret<0){
            packet_queue_read_finish(&vc->video_pq);
            packet_queue_read_finish(&vc->audio_pq);
            vc->read_eof=1;
            continue;
        }

        if(pkt->stream_index==vc->audio_index){
            packet_queue_put(&vc->audio_pq,pkt);
        }else if(pkt->stream_index==vc->video_index){
            packet_queue_put(&vc->video_pq,pkt);
        }
        av_packet_unref(pkt);
    }

    ret=0;
_error:
    /* ★ 不能在这里关 Fmtctx：vc->Vstream/Astream 指向它内部的 AVStream，
       此刻 video/audio 解码线程还在用它们（synchronize_video 取 time_base、PTS 换算），
       这里关掉就是 use-after-free → 切视频时段错误。
       统一在 VideoContainer_deinit 里等所有线程 join 完再 avformat_close_input(&vc->ifmtctx)。 */
    pthread_mutex_destroy(&wait_mutex);
    if(pkt) av_packet_free(&pkt);
    return NULL;
}

static VideoContainer* VideoContainer_init(char *filename)
{
    VideoContainer *vc;

    vc=av_mallocz(sizeof(VideoContainer));

    if(!vc){
        av_log(NULL,AV_LOG_ERROR,"Fail to allocate memory!\n");
        goto _error;
    }

    if(packet_queue_init(&vc->audio_pq)<0||packet_queue_init(&vc->video_pq)<0)
        goto _error;

    if(video_frame_queue_init(&vc->video_fq,VIDEO_FRAME_QUEUE_SIZE)<0)
        goto _error;

    if(audio_frame_queue_init(&vc->audio_fq,VIDEO_FRAME_QUEUE_SIZE)<0)
        goto _error;

    pthread_cond_init(&continue_read_cond,NULL);

    pthread_mutex_init(&vc->codec_mutex,NULL);
    vc->queues_ready = 1;   /* 队列/互斥/条件变量都已就绪，deinit 可安全释放 */

    vc->filename=av_strdup(filename);
    vc->volume=g_volume;   /* 沿用当前音量（初始 0.35） */
    vc->audio_index=vc->video_index=-1;
    pthread_create(&vc->read_tid,NULL,read_thread,vc);

    if(!vc->read_tid){
        av_log(NULL,AV_LOG_FATAL,"read_thread create error!\n");
    }

    clock_init(&vc->audclk);
    clock_init(&vc->vidclk);

    return vc;
_error:
    VideoContainer_deinit(vc);
    return NULL;
}

/* ---------- music_list：MP4 文件列表 ---------- */

// 点击列表项：播放该对应视频（事件回调持有 lv_lock，只挂起切换请求）
static void music_list_item_click_cb(lv_event_t * e)
{
    lv_obj_t * lbl = lv_event_get_target(e);
    const char * path = lv_obj_get_user_data(lbl);
    if(!path || !*path) return;
    video_switch_to_file(path);
}

// 把播放列表里的文件以 20 号文本标签填进 music_list；只在第一次获得播放文件时创建
static void playlist_populate_music_list(void)
{
    lv_obj_t * list = guider_ui.screen_1.music_list;
    if(!list) return;
    if(lv_obj_get_child_count(list) > 0) return;   /* 已建过，避免重复 */

    for(file_node * p = head; p; p = p->next){
        const char * name = get_file_last_name(p->filename);
        lv_obj_t * lbl = lv_list_add_text(list, name);
        lv_obj_set_style_text_font(lbl, &lv_font_montserratMedium_20, 0);   /* 20 号字体 */
        lv_obj_set_clickable(lbl, true);
        lv_obj_set_user_data(lbl, av_strdup(p->filename));   /* 点击时能拿到完整路径 */
        lv_obj_add_event_cb(lbl, music_list_item_click_cb, LV_EVENT_CLICKED, NULL);

        /* 文件名超宽：单行 + 左右循环滚动（marquee），而不是换行把标签撑高 */
        lv_obj_set_width(lbl, lv_pct(100));                  /* 固定为列表宽度，文本超宽才滚动 */
        lv_label_set_long_mode(lbl, LV_LABEL_LONG_SCROLL_CIRCULAR);
        lv_obj_set_style_anim_duration(lbl, lv_anim_speed(30), 0);   /* 默认 40px/s 放慢 0.75 倍 → 30px/s */

        /* 每个标签加边框 + 内边距，区分标签边界 */
        lv_obj_set_style_border_width(lbl, 1, 0);
        lv_obj_set_style_border_color(lbl, lv_color_hex(0x606060), 0);
        lv_obj_set_style_border_side(lbl, LV_BORDER_SIDE_FULL, 0);
        lv_obj_set_style_pad_left(lbl, 6, 0);
        lv_obj_set_style_pad_right(lbl, 6, 0);
        lv_obj_set_style_pad_top(lbl, 2, 0);
        lv_obj_set_style_pad_bottom(lbl, 2, 0);
    }
}

// 清空 music_list 的标签并释放各自 strdup 的路径（退出播放器时调用；重新进入会重建）
static void music_list_clear(void)
{
    lv_obj_t * list = guider_ui.screen_1.music_list;
    if(!list) return;
    uint32_t n = lv_obj_get_child_count(list);
    for(uint32_t i = 0; i < n; i++){
        lv_obj_t * child = lv_obj_get_child(list, i);
        const char * path = lv_obj_get_user_data(child);
        if(path) av_free((char *)path);
    }
    lv_obj_clean(list);   /* 标签由列表持有，user_data 路径已在上方手动释放 */
}

// 全局播放器实例（供事件回调控制）
static VideoContainer *g_vc = NULL;

/* ---------- 播放进度条 + 时间显示 ---------- */

static lv_timer_t *g_progress_timer = NULL;
static int  g_slider_dragging = 0;      /* 用户正拖进度条：程序定时刷新跳过 */
static double g_slider_duration = -1;   /* 当前进度条量程对应的视频时长（秒），换了视频就重设 */

static void format_hms(char *buf, size_t n, double sec)
{
    if(sec < 0) sec = 0;
    int total = (int)sec;
    snprintf(buf, n, "%02d:%02d:%02d", total / 3600, (total / 60) % 60, total % 60);
}

// 进度条拖动：绝对 seek 到指定秒数（区别于 video_seek 的相对增减）
void video_seek_to(double seconds)
{
    if(!g_vc) return;
    if(seconds < 0) seconds = 0;
    double duration = 0;
    if(g_vc->ifmtctx && g_vc->ifmtctx->duration > 0)
        duration = (double)g_vc->ifmtctx->duration / AV_TIME_BASE;
    double end_margin = 1.0;   /* 末尾留白 1 秒，避免 seek 到正好末尾立即 EOF */
    if(duration > 0 && seconds > duration - end_margin)
        seconds = duration - end_margin;
    else if(duration <= 0)
        seconds = 0;
    g_vc->seek_pos = (uint64_t)(seconds * AV_TIME_BASE);
    g_vc->seek_req = 1;
}

// 定时刷新（250ms）：总时长 + 已播时长 + 进度条位置
static void progress_timer_cb(lv_timer_t *t)
{
    lv_obj_t *slider    = guider_ui.screen_1.container_1_time_slider;
    lv_obj_t *l_total   = guider_ui.screen_1.container_1_label_1;
    lv_obj_t *l_elapsed = guider_ui.screen_1.container_1_label_2;
    if(!slider || !l_total || !l_elapsed) return;
    if(!g_vc || !g_vc->ifmtctx) return;

    double dur = g_vc->ifmtctx->duration > 0
                 ? (double)g_vc->ifmtctx->duration / AV_TIME_BASE : 0;

    /* 换视频/首次打开：按新时长重设进度条量程（1 格 = 1 秒） */
    if(dur > 0 && dur != g_slider_duration){
        g_slider_duration = dur;
        lv_slider_set_range(slider, 0, (int)dur);
    }
    char buf[16];
    format_hms(buf, sizeof(buf), dur);
    lv_label_set_text(l_total, buf);

    if(g_slider_dragging) return;   /* 拖动中：进度条和已播时长由手指控制 */

    double cur = get_master_clock(g_vc);
    /* 音频已结束、视频还在播（音轨比画面短）：进度条继续用视频时钟走 */
    if(g_vc->audio_eof && !g_vc->video_eof && !isnan(g_vc->video_clock))
        cur = g_vc->video_clock;
    if(isnan(cur)) cur = 0;

    format_hms(buf, sizeof(buf), cur);
    lv_label_set_text(l_elapsed, buf);

    if(dur > 0){
        int32_t val = (int32_t)cur;
        if(val < 0) val = 0;
        if(val > (int32_t)dur) val = (int32_t)dur;
        lv_slider_set_value(slider, val, LV_ANIM_OFF);
    }
}

// 进度条事件：PRESSED 进入拖动态，拖动中只跟手刷新已播时长，RELEASED 才真正 seek
static void time_slider_event_cb(lv_event_t *e)
{
    lv_event_code_t code = lv_event_get_code(e);
    lv_obj_t *slider = guider_ui.screen_1.container_1_time_slider;
    char buf[16];

    if(code == LV_EVENT_PRESSED){
        g_slider_dragging = 1;
    } else if(code == LV_EVENT_VALUE_CHANGED){
        if(!g_slider_dragging) return;
        double sec = lv_slider_get_value(slider);
        format_hms(buf, sizeof(buf), sec);
        lv_label_set_text(guider_ui.screen_1.container_1_label_2, buf);
    } else if(code == LV_EVENT_RELEASED){
        if(!g_slider_dragging) return;
        g_slider_dragging = 0;
        double sec = lv_slider_get_value(slider);
        video_seek_to(sec);
    } else if(code == LV_EVENT_PRESS_LOST){
        g_slider_dragging = 0;   /* 按压被取消（如列表滚动误触）：只退出拖动态，不 seek */
    }
}

// 初始化进度条 UI（定时器 + 拖动事件，只做一次）
static void progress_ui_init(void)
{
    if(g_progress_timer) return;
    lv_label_set_text(guider_ui.screen_1.container_1_label_1, "00:00:00");
    lv_label_set_text(guider_ui.screen_1.container_1_label_2, "00:00:00");
    g_progress_timer = lv_timer_create(progress_timer_cb, 250, NULL);
    lv_obj_add_event_cb(guider_ui.screen_1.container_1_time_slider,
                        time_slider_event_cb, LV_EVENT_ALL, NULL);
}

// 启动视频播放：绑定 LVGL image_1 + 初始化播放器线程
void video_start(const char *path)
{
    if (g_vc) return;   // 已经在播

    // 绑定 LVGL 显示目标
    g_image = guider_ui.screen_1.image_1;

    // 初始化音量控件：默认值 1，初始 = 1*0.35 = 0.35（35%）
    int init_vol = (int)(g_volume * 100.0f);
    lv_slider_set_value(guider_ui.screen_1.container_1_volume_slider, init_vol, LV_ANIM_OFF);
    lv_label_set_text_fmt(guider_ui.screen_1.container_1_volume_value_label, "volume:%d", init_vol);

    // 第一次获得播放文件：扫描同目录建播放列表，并把 MP4 文件以文本标签填进 music_list
    if(head == NULL){
        char *dir = get_parent_dir(path);
        playlist_build(dir);
        av_free(dir);
    }
    playlist_populate_music_list();
    progress_ui_init();   /* 进度条定时器 + 拖动事件（只注册一次） */

    g_vc = VideoContainer_init(path);
    if (!g_vc) {
        av_log(NULL, AV_LOG_ERROR, "video_start: init failed\n");
    }
}

// 停止播放：先唤醒所有线程再 join（不能在持有 lv_lock 时调用）
void video_stop(void)
{
    if (!g_vc) return;
    player_shutdown(g_vc);
    VideoContainer_deinit(g_vc);
    g_vc = NULL;
}

/* 唤醒所有可能阻塞的线程（置 quit + abort + signal），join 交给 deinit */
static void player_shutdown(VideoContainer *vc)
{
    if(!vc) return;
    vc->quit = 1;
    packet_queue_abort(&vc->audio_pq);
    packet_queue_abort(&vc->video_pq);

    /* 唤醒阻塞在帧队列上的播放/解码线程 */
    pthread_mutex_lock(&vc->video_fq.mutex);
    vc->video_fq.abort_request = 1;
    pthread_cond_signal(&vc->video_fq.cond);
    pthread_mutex_unlock(&vc->video_fq.mutex);

    pthread_mutex_lock(&vc->audio_fq.mutex);
    vc->audio_fq.abort_request = 1;
    pthread_cond_signal(&vc->audio_fq.cond);
    pthread_mutex_unlock(&vc->audio_fq.mutex);

    /* 唤醒 read_thread 的队列满等待 */
    pthread_cond_signal(&continue_read_cond);
}

/* ---------- 对外控制接口（供 GUI 事件回调调用） ---------- */

// 快进/回退（秒，可为负）——仅置 seek 请求，非阻塞
void video_seek(double seconds)
{
    if(!g_vc) return;
    stream_seek(g_vc, seconds);
}

// 暂停/继续
void video_toggle_pause(void)
{
    if(!g_vc) return;
    if(g_vc->paused)
        stream_resume(g_vc);
    else
        stream_paused(g_vc);
}

// 设置音量（0~100 整数百分比）：滑动条拖动时回调调用，实时生效
void video_set_volume(int percent)
{
    if(percent < 0) percent = 0;
    if(percent > 100) percent = 100;
    g_volume = percent / 100.0f;
    if(g_vc) g_vc->volume = g_volume;
}

// 请求切换视频：direction = VIDEO_LAST(0) 上一部 / VIDEO_NEXT(1) 下一部
// 事件回调里调用（此时持有 lv_lock）：只挂起请求，真正的线程切换在 video_process_switch
void video_switch(int direction)
{
    if(!g_vc) return;

    if(head == NULL){
        char *dir = get_parent_dir(g_vc->filename);
        playlist_build(dir);
        av_free(dir);
    }
    if(!playlist_find(g_vc->filename)){
        /* 当前文件不在列表里（目录可能变化），重建后重找 */
        char *dir = get_parent_dir(g_vc->filename);
        playlist_build(dir);
        av_free(dir);
        playlist_find(g_vc->filename);
    }

    char *next = select_next_file(head_now, direction);
    if(!next || !strcmp(next, g_vc->filename)) return;   /* 只有一部片或切无可切 */

    if(g_switch_filename) av_free(g_switch_filename);
    g_switch_filename = av_strdup(next);
    g_switch_pending = 1;
}

// 请求切到指定文件（music_list 点击用）：只挂起，真正的线程切换在 video_process_switch
void video_switch_to_file(const char *path)
{
    if(!g_vc || !path || !*path) return;
    if(!strcmp(path, g_vc->filename)) return;   /* 正在播的就是它 */

    if(g_switch_filename) av_free(g_switch_filename);
    g_switch_filename = av_strdup(path);
    g_switch_pending = 1;
}

// 主循环（无 lv_lock 时）调用：执行真正的停旧起新
void video_process_switch(void)
{
    if(!g_switch_pending) return;
    g_switch_pending = 0;
    if(!g_vc || !g_switch_filename) return;

    VideoContainer *old = g_vc;
    char *newfile = g_switch_filename;
    g_switch_filename = NULL;

    player_shutdown(old);
    VideoContainer_deinit(old);
    g_vc = NULL;

    g_vc = VideoContainer_init(newfile);
    av_free(newfile);
    if(!g_vc) av_log(NULL, AV_LOG_ERROR, "video_switch: init new file failed\n");
}

/* ---------- 播放器退出（button_1） ---------- */

static int g_exit_requested = 0;

// 退出按钮点击回调（持 lv_lock）：只置请求标志 + 清掉挂起的切换。真正的停止放主循环无锁区，
// 因为 video_stop() 要 join 播放线程，而播放线程可能正阻塞在 lv_lock() 上 —— 持锁 join 会死锁。
void video_player_exit(void)
{
    g_exit_requested = 1;
    if(g_switch_filename){ av_free(g_switch_filename); g_switch_filename = NULL; }
    g_switch_pending = 0;   /* 挂起的切视频不再执行 */
}

// 主循环（无 lv_lock 时）调用：停线程 + 释放所有资源。退出后回到 screen，可再次进入播放器。
void video_player_process_exit(void)
{
    if(!g_exit_requested) return;
    g_exit_requested = 0;

    video_stop();            /* 停线程、释放解码器/队列/ifmtctx（不能在持 lv_lock 时调用） */
    playlist_free();         /* 释放播放列表链表 */
    music_list_clear();      /* 清空列表标签并释放 strdup 的路径（重新进入会重建） */
    g_slider_duration = -1;  /* 下次进入按新视频重设进度条量程 */
    g_slider_dragging = 0;   /* 若退出时正拖着进度条，标志复位 */
}

static void VideoContainer_deinit(VideoContainer *vc)
{
    if(!vc) return;

    /* init 成功后才销毁队列/互斥/条件变量：init 中途失败时这些可能还没初始化，
       对未初始化的 pthread 对象调用 destroy 是未定义行为。 */
    if(vc->queues_ready){
        if(vc->read_tid) pthread_join(vc->read_tid,NULL);
        if(vc->audio_decode_tid) pthread_join(vc->audio_decode_tid,NULL);
        if(vc->audio_play_tid) pthread_join(vc->audio_play_tid,NULL);
        if(vc->video_decode_tid) pthread_join(vc->video_decode_tid,NULL);
        if(vc->video_play_tid) pthread_join(vc->video_play_tid,NULL);
        pthread_cond_destroy(&continue_read_cond);
        if(vc->Actx) avcodec_free_context(&vc->Actx);
        if(vc->Vctx) avcodec_free_context(&vc->Vctx);
        if(vc->Aframe) av_frame_free(&vc->Aframe);   /* 之前漏了，每次切换都泄漏 */
        if(vc->Vpkt) av_packet_free(&vc->Vpkt);      /* 之前漏了，每次切换都泄漏 */
        packet_queue_destroy(&vc->audio_pq);
        packet_queue_destroy(&vc->video_pq);
        video_frame_queue_destory(&vc->video_fq);
        audio_frame_queue_destroy(&vc->audio_fq);
        if(vc->ifmtctx) avformat_close_input(&vc->ifmtctx);
        pthread_mutex_destroy(&vc->codec_mutex);
    }
    if(vc->swr_ctx) swr_free(&vc->swr_ctx);
    if(vc->filename) av_free(vc->filename);
    av_freep(&vc);
}

