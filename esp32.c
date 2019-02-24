#include <errno.h>
#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include <freertos/task.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "sqlite3.h"


#ifndef SQLITE_ESP32VFS_BUFFERSZ
# define SQLITE_ESP32VFS_BUFFERSZ 8192
#endif

typedef struct {
    sqlite3_file base;
    FILE *f;

    sqlite3_int64 size;

    char *aBuffer;
    int nBuffer;
    sqlite3_int64 iBufferOfst;
} esp32_file_t;


static int esp32_direct_write(esp32_file_t *p, const void *zBuf, int iAmt, sqlite_int64 iOfst)
{
    if (fseek(p->f, iOfst, SEEK_SET) < 0) {
        return SQLITE_IOERR;
    }

    size_t n = fwrite(zBuf, 1, iAmt, p->f);
    if (n != iAmt) {
        return SQLITE_IOERR;
    }

    if (iOfst + iAmt > p->size) {
        p->size = iOfst + iAmt;
    }

    return SQLITE_OK;
}


static int esp32_flush_buffer(esp32_file_t *p)
{
    int rc = SQLITE_OK;
    if (p->aBuffer) {
        rc = esp32_direct_write(p, p->aBuffer, p->nBuffer, p->iBufferOfst);
        p->nBuffer = 0;
    }
    return rc;
}


static int esp32_close(sqlite3_file *pFile)
{
    int rc;
    esp32_file_t *p = (esp32_file_t *)pFile;
    rc = esp32_flush_buffer(p);
    fclose(p->f);
    return rc;
}


static int esp32_read(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite_int64 iOfst)
{
    esp32_file_t *p = (esp32_file_t *)pFile;

    int rc = esp32_flush_buffer(p);
    if (rc != SQLITE_OK) {
        return rc;
    }

    if (fseek(p->f, iOfst, SEEK_SET) < 0) {
        return SQLITE_IOERR_READ;
    }

    size_t n = fread(zBuf, 1, iAmt, p->f);
    if (n == iAmt) {
        return SQLITE_OK;
    } else if (ferror(p->f)) {
        clearerr(p->f);
        return SQLITE_IOERR_READ;
    }

    return SQLITE_IOERR_SHORT_READ;
}


static int esp32_write(sqlite3_file *file, const void *zBuf, int iAmt, sqlite3_int64 iOfst)
{
    esp32_file_t *p = (esp32_file_t *)file;

    if (!p->aBuffer) {
        return esp32_direct_write(p, zBuf, iAmt, iOfst);
    }

    char *z = (char *)zBuf;
    int n = iAmt;
    sqlite3_int64 i = iOfst;

    while (n > 0) {
        if (p->nBuffer == SQLITE_ESP32VFS_BUFFERSZ || p->iBufferOfst + p->nBuffer != i) {
            int rc = esp32_flush_buffer(p);
            if (rc != SQLITE_OK) {
                return rc;
            }
        }
        assert(p->nBuffer == 0 || p->iBufferOfst + p->nBuffer == i);
        p->iBufferOfst = i - p->nBuffer;

        int nCopy = SQLITE_ESP32VFS_BUFFERSZ - p->nBuffer;
        if (nCopy > n) {
            nCopy = n;
        }
        memcpy (&p->aBuffer[p->nBuffer], z, nCopy);
        p->nBuffer += nCopy;

        n -= nCopy;
        i += nCopy;
        z += nCopy;
    }

    return SQLITE_OK;
}


static int esp32_truncate(sqlite3_file *pFile, sqlite_int64 size)
{
    return SQLITE_OK;
}


static int esp32_sync(sqlite3_file *pFile, int flags)
{
    esp32_file_t *p = (esp32_file_t *)pFile;

    int rc = esp32_flush_buffer(p);
    if (rc != SQLITE_OK) {
        return rc;
    }

    rc = fflush(p->f);
    return (rc == 0 ? SQLITE_OK : SQLITE_IOERR_FSYNC);
}


static int esp32_file_size(sqlite3_file *pFile, sqlite_int64 *pSize)
{
    esp32_file_t *p = (esp32_file_t *) pFile;

    int rc = esp32_flush_buffer(p);
    if (rc != SQLITE_OK) {
        return rc;
    }

    *pSize = p->size;
    return SQLITE_OK;
}


static int esp32_lock(sqlite3_file *pFile, int eLock)
{
    return SQLITE_OK;
}


static int esp32_unlock(sqlite3_file *pFile, int eLock)
{
    return SQLITE_OK;
}


static int esp32_check_reserved_lock(sqlite3_file *pFile, int *pResOut)
{
    *pResOut = 0;
    return SQLITE_OK;
}


static int esp32_file_control(sqlite3_file *pFile, int op, void *pArg)
{
    return SQLITE_OK;
}


static int esp32_sector_size(sqlite3_file *pFile)
{
    return 512; /* FIXME: detect this */
}


static int esp32_device_characteristics(sqlite3_file *pFile)
{
    return 0;
}


static int esp32_open(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags)
{
    static const sqlite3_io_methods esp32_io = {
        .iVersion = 1,
        .xClose = esp32_close,
        .xWrite = esp32_write,
        .xRead = esp32_read,
        .xTruncate = esp32_truncate,
        .xSync = esp32_sync,
        .xFileSize = esp32_file_size,
        .xLock = esp32_lock,
        .xUnlock = esp32_unlock,
        .xCheckReservedLock = esp32_check_reserved_lock,
        .xFileControl = esp32_file_control,
        .xSectorSize = esp32_sector_size,
        .xDeviceCharacteristics = esp32_device_characteristics,
    };

    esp32_file_t *p = (esp32_file_t *)pFile;
    char *aBuf = NULL;
    char *mode = "r";

    if (zName == NULL) {
        return SQLITE_IOERR;
    }

    if (0 && flags & SQLITE_OPEN_MAIN_JOURNAL) {
        aBuf = (char *)sqlite3_malloc(SQLITE_ESP32VFS_BUFFERSZ);
        if (!aBuf) {
            return SQLITE_NOMEM;
        }
    }

    bool exist = true;
    struct stat st;
    if (stat(zName, &st) < 0) {
        exist = false;
    }

    if (!exist && !(flags & SQLITE_OPEN_CREATE)) {
        sqlite3_free(aBuf);
        return SQLITE_CANTOPEN;
    }

    if (flags & SQLITE_OPEN_READWRITE) {
        mode = exist ? "r+" : "w+";
    }

    memset(p, 0, sizeof(esp32_file_t));
    p->f = fopen(zName, mode);
    if (p->f == NULL) {
        sqlite3_free(aBuf);
        return SQLITE_CANTOPEN;
    }
    p->aBuffer = aBuf;
    p->size = exist ? st.st_size : 0;

    if (pOutFlags) {
        *pOutFlags = flags;
    }
    p->base.pMethods = &esp32_io;
    return SQLITE_OK;
}


static int esp32_delete(sqlite3_vfs *pVfs, const char *zPath, int dirSync)
{
    int rc;

    rc = remove(zPath);
    if (rc == 0 || (rc != 0 && errno == ENOENT)) {
        return SQLITE_OK;
    }
    return SQLITE_IOERR_DELETE;
}


static int esp32_access(sqlite3_vfs *pVfs, const char *zPath, int flags, int *pResOut)
{
    struct stat st;
    *pResOut = (stat(zPath, &st) >= 0);
    return SQLITE_OK;
}


static int esp32_full_pathname(sqlite3_vfs *pVfs, const char *zPath, int nPathOut, char *zPathOut)
{
    if (zPath[0] != '/') {
        sqlite3_snprintf(nPathOut, zPathOut, "/%s", zPath);
    } else {
        strncpy(zPathOut, zPath, nPathOut);
    }
    zPathOut[nPathOut - 1] = '\0';

    return SQLITE_OK;
}


static void *esp32_dl_open(sqlite3_vfs *pVfs, const char *zPath)
{
    return NULL;
}


static void esp32_dl_error(sqlite3_vfs *pVfs, int nByte, char *zErrMsg)
{
    sqlite3_snprintf(nByte, zErrMsg, "Dynamic lodable extensions are not supported");
    zErrMsg[nByte - 1] = '\0';
}


static void (*esp32_dl_sym(sqlite3_vfs *pVfs, void *pHandle, const char *zSym))(void)
{
    return NULL;
}


static void esp32_dl_close(sqlite3_vfs *pVfs, void *pHandle) 
{
    return;
}


static int esp32_randomness(sqlite3_vfs *pVfs, int nByte, char *zByte)
{
    int dword_count = (nByte + 3) / sizeof(uint32_t);
    uint32_t buf[dword_count];

    for (size_t i = 0; i < dword_count; i++) {
        buf[i] = esp_random();
    }

    memcpy(zByte, buf, nByte);
    return nByte;
}


static int esp32_sleep(sqlite3_vfs *pVfs, int nMicro)
{
    TickType_t start = xTaskGetTickCount();
    vTaskDelay(nMicro / 1000 / portTICK_PERIOD_MS);
    return ((xTaskGetTickCount() - start) * portTICK_PERIOD_MS) * 1000;
}


static int esp32_current_time(sqlite3_vfs *pVfs, double *pTime)
{
    time_t t = time(0);
    *pTime = t / 86400.0 + 2440587.5;
    return SQLITE_OK;
}


static int esp32_get_last_error(sqlite3_vfs *pVfs, int nErrorOut, char *zErrorOut)
{
    return errno;
}


static int esp32_current_time_int64(sqlite3_vfs *pVfs, sqlite3_int64 *piNow)
{
    static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
    struct timeval sNow;

    (void)gettimeofday(&sNow, 0);
    *piNow = unixEpoch + 1000*(sqlite3_int64)sNow.tv_sec + sNow.tv_usec/1000;
    return SQLITE_OK;
}


int sqlite3_os_init(void)
{
    static sqlite3_vfs esp32_vfs = {
        .iVersion = 2,
        .szOsFile = sizeof(esp32_file_t),
        .mxPathname = PATH_MAX + 1,
        .pNext = NULL,
        .zName = "esp32",
        .pAppData = NULL,
        .xOpen = esp32_open,
        .xDelete = esp32_delete,
        .xAccess = esp32_access,
        .xFullPathname = esp32_full_pathname,
        .xDlOpen = esp32_dl_open,
        .xDlError = esp32_dl_error,
        .xDlSym = esp32_dl_sym,
        .xDlClose = esp32_dl_close,
        .xRandomness = esp32_randomness,
        .xSleep = esp32_sleep,
        .xCurrentTime = esp32_current_time,
        .xGetLastError = esp32_get_last_error,
        .xCurrentTimeInt64 = esp32_current_time_int64,
    };

    sqlite3_vfs_register(&esp32_vfs, 1);
    return SQLITE_OK;
}


int sqlite3_os_end(void)
{
    return SQLITE_OK;
}


struct sqlite3_mutex {
    xSemaphoreHandle mutex;
    int id;
};


static int freertosMutexInit(void)
{
    return SQLITE_OK;
}


static int freertosMutexEnd(void)
{
    return SQLITE_OK;
}


static sqlite3_mutex *freertosMutexAlloc(int id)
{
    sqlite3_mutex *p = calloc(1, sizeof(*p));
    if (!p) {
        return NULL;
    }
    p->id = id;

    switch (id) {
    case SQLITE_MUTEX_RECURSIVE:
        p->mutex = xSemaphoreCreateRecursiveMutex();
        break;

    case SQLITE_MUTEX_FAST:
    default:
        p->mutex = xSemaphoreCreateMutex();
        break;
    }

    return p;
}


static void freertosMutexFree(sqlite3_mutex *p)
{
    vSemaphoreDelete(p->mutex);
    free(p);
    return;
}


static void freertosMutexEnter(sqlite3_mutex *p)
{
    if (p->id == SQLITE_MUTEX_RECURSIVE) {
        xSemaphoreTakeRecursive(p->mutex, portMAX_DELAY);
    } else {
        xSemaphoreTake(p->mutex, portMAX_DELAY);
    }
}


static int freertosMutexTry(sqlite3_mutex *p)
{
    int rc;
    if (p->id == SQLITE_MUTEX_RECURSIVE) {
        rc = xSemaphoreTakeRecursive(p->mutex, 0);
    } else {
        rc = xSemaphoreTake(p->mutex, 0);
    }
    return rc == pdTRUE ? SQLITE_OK : SQLITE_BUSY;
}


static void freertosMutexLeave(sqlite3_mutex *p)
{
    if (p->id == SQLITE_MUTEX_RECURSIVE) {
        xSemaphoreGiveRecursive(p->mutex);
    } else {
        xSemaphoreGive(p->mutex);
    }
}


static int freertosMutexHeld(sqlite3_mutex *p)
{
    return xSemaphoreGetMutexHolder(p->mutex) != NULL;
}


static int freertosMutexNotHeld(sqlite3_mutex *p)
{
    return xSemaphoreGetMutexHolder(p->mutex) == NULL;
}


sqlite3_mutex_methods const *sqlite3FreertosMutex(void)
{
    static const sqlite3_mutex_methods sMutex = {
        freertosMutexInit,
        freertosMutexEnd,
        freertosMutexAlloc,
        freertosMutexFree,
        freertosMutexEnter,
        freertosMutexTry,
        freertosMutexLeave,
        freertosMutexHeld,
        freertosMutexNotHeld,
    };

    return &sMutex;
}
