#include "MmapUtil.h"
#include "jni.h"
#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>

JNIEXPORT void JNICALL Java_qunar_tc_qmq_store_MmapUtil_free0(JNIEnv * env, jclass clazz, jint fd, jlong offset, jlong len)
{
        int result = posix_fadvise((int)fd, (off_t)offset, (off_t)len, POSIX_FADV_DONTNEED);
        if (result == -1) {
                jclass cls = (*env)->FindClass(env, "java/io/IOException");
                if(cls != 0)
                        (*env)->ThrowNew(env, cls, "madvise error");
        }
}