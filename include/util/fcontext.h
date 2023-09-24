/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2022-2023 Feng Ren, Tsinghua University 
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#ifndef SDS_FCONTEXT_H
#define SDS_FCONTEXT_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *fcontext_t;

typedef struct {
    fcontext_t fctx;
    void *data;
} transfer_t;

fcontext_t make_fcontext(void *sp, size_t size, void(*fn)(transfer_t));
transfer_t jump_fcontext(fcontext_t const to, void *vp);
transfer_t ontop_fcontext(fcontext_t const to, void *vp, transfer_t(*fn)(transfer_t));

#ifdef __cplusplus
}
#endif

#endif //SDS_FCONTEXT_H
