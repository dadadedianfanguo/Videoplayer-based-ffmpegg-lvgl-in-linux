#include "lvgl/lvgl.h"
#include "lvgl/demos/lv_demos.h"
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <signal.h>
#include <fcntl.h>
#include <generated/gg_utils.h>
#include <generated/gui_guider.h>
#include "lvgl/src/drivers/evdev/lv_evdev.h"

void video_process_switch(void);   // videoplayer.c：主循环无锁区执行的视频切换
void video_player_process_exit(void);   // videoplayer.c：主循环无锁区执行的播放器退出（停线程/释放资源）

/* SIGSEGV/SIGABRT 回溯：把崩溃时的调用栈写到 /mnt/udisk/crash.log，便于定位 */
static void crash_handler(int sig)
{
    void *bt[32];
    int n = backtrace(bt, 32);
    int fd = open("/mnt/udisk/crash.log", O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd >= 0) {
        char msg[96];
        int len = snprintf(msg, sizeof(msg), "\n=== SIG %d (%d frames) ===\n", sig, n);
        write(fd, msg, len);
        backtrace_symbols_fd(bt, n, fd);
        write(fd, "\n", 1);
        close(fd);
    }
    dprintf(2, "[crash] SIG %d, %d frames -> /mnt/udisk/crash.log\n", sig, n);
    _exit(1);
}

static void install_crash_handlers(void)
{
    signal(SIGSEGV, crash_handler);
    signal(SIGABRT, crash_handler);
    signal(SIGBUS,  crash_handler);
    signal(SIGILL,  crash_handler);
    signal(SIGFPE,  crash_handler);
}

static const char *getenv_default(const char *name, const char *dflt)
{
    return getenv(name) ? : dflt;
}

#if LV_USE_LINUX_FBDEV
static void lv_linux_disp_init(void)
{
    const char *device = getenv_default("LV_LINUX_FBDEV_DEVICE", "/dev/fb0");
    lv_display_t * disp = lv_linux_fbdev_create();

    lv_linux_fbdev_set_file(disp, device);
    lv_linux_fbdev_set_force_refresh(disp, false);   /* 只刷脏区域，提高渲染吞吐，不再每帧强制全屏刷新 */

    /* LVGL 默认刷新周期 33ms≈30fps，降到 16ms 放开到 60fps 上限 */
    lv_timer_set_period(lv_display_get_refr_timer(disp), 16);

    const char *touch = getenv_default("LV_LINUX_TOUCH_DEVICE", "/dev/input/event0");
    lv_indev_t * indev = lv_evdev_create(LV_INDEV_TYPE_POINTER, touch);
    if (indev) {
        // 不设校准：evdev 自动从设备读取触摸坐标范围
        // lv_evdev_set_calibration(indev, 0, 0, 800, 480);
    }
}
#elif LV_USE_LINUX_DRM
static void lv_linux_disp_init(void)
{
    const char *device = getenv_default("LV_LINUX_DRM_CARD", "/dev/dri/card0");
    lv_display_t * disp = lv_linux_drm_create();

    lv_linux_drm_set_file(disp, device, -1);
}
#elif LV_USE_SDL
static void lv_linux_disp_init(void)
{
    const int width = atoi(getenv("LV_SDL_VIDEO_WIDTH") ? : "800");
    const int height = atoi(getenv("LV_SDL_VIDEO_HEIGHT") ? : "480");

    lv_sdl_window_create(width, height);

    /* 鼠标、滚轮、键盘 */
    lv_sdl_mouse_create();
    lv_sdl_mousewheel_create();
    lv_sdl_keyboard_create();    

}
#else
#error Unsupported configuration
#endif

int main(void)
{
    lv_init();

    /*Linux display device init*/
    lv_linux_disp_init();

    /* 崩溃回溯：任何线程段错误都记录调用栈到 /mnt/udisk/crash.log */
    install_crash_handlers();

    /*Initialize the UI*/
    setup_ui(&guider_ui);
       

    /*Create a Demo*/
    //lv_demo_widgets();
    //lv_demo_widgets_start_slideshow();

    /*Handle LVGL tasks*/
    while(1) {
        lv_lock();
        lv_timer_handler();
        lv_unlock();
        video_process_switch();   /* 切视频的线程停/起：必须在无 lv_lock 时做，否则和播放线程抢锁死锁 */
        video_player_process_exit();   /* 退出播放器：join 线程/释放资源，同样必须在无 lv_lock 时做 */
        usleep(5000);
    }

    return 0;
}
