/*
 * Copyright (c) 2011, Liexusong <liexusong@qq.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "dlist.h"

struct list *list_init(list_destroy_function_t destroy, int cache_size)
{
    struct list *list;
    
    if (!destroy) {
        fprintf(stderr, "FATAL: DoubleList destroy function undefined!\n");
        return NULL;
    }
    
    list = (struct list *)malloc(sizeof(struct list));
    if (list)
    {
        list->head = NULL;
        list->tail = NULL;
        list->size = 0;
        list->cache_used = 0;
        list->cache_size = cache_size > 0 ? cache_size : 0;
        list->destroy = destroy;
        
        if (cache_size > 0) {
            list->caches = (struct list_node **)malloc(sizeof(struct list_node *) * cache_size);
            if (!list->caches) {
                free(list);
                return NULL;
            }
        } else {
            list->caches = NULL;
        }
    }
    
    return list;
}

static struct list_node *item_alloc(struct list *list)
{
    struct list_node *node;
    
    if (list->cache_size && list->cache_used > 0) {
        node = list->caches[--list->cache_used];
    } else {
        node = (struct list_node *)malloc(sizeof(struct list_node));
    }
    
    return node;
}

static void item_remove(struct list *list, struct list_node *node)
{
    if (list->cache_size && list->cache_used < list->cache_size) {
        list->caches[list->cache_used++] = node;
    } else {
        free(node);
    }
}

int list_add_head(struct list *list, void *value)
{
    struct list_node *node;
    
    node = item_alloc(list);
    if (!node)
        return -1;
    node->prev = NULL;
    node->next = list->head;
    node->value = value;
    
    if (list->head)
        list->head->prev = node;
    if (!list->tail)
        list->tail = node;
    list->head = node;
    list->size++;
    return 0;
}

int list_add_tail(struct list *list, void *value)
{
    struct list_node *node;
    
    node = item_alloc(list);
    if (!node)
        return -1;
    node->next = NULL;
    node->prev = list->tail;
    node->value = value;
    
    if (list->tail)
        list->tail->next = node;
    if (!list->head)
        list->head = node;
    list->tail = node;
    list->size++;
    return 0;
}

int list_add_index(struct list *list, void *value, int position)
{
    struct list_node *node, *newnode;
    
    if (position == 0)
        return list_add_head(list, value);
    if (position == list->size)
        return list_add_tail(list, value);
    
    if (position < 0) {
        position = (-position) - 1;
        node = list->tail;
        while (position > 0 && node) {
            node = node->prev;
            position--;
        }
    } else {
        node = list->head;
        while (position > 0 && node) {
            node = node->next;
            position--;
        }
    }
    
    if (node) {
        newnode = item_alloc(list);
        if (!newnode)
            return -1;
        if (node->prev)
            node->prev->next = newnode;
        else
            list->head = newnode;
        newnode->prev = node->prev;
        newnode->next = node;
        node->prev = newnode;
        return 0;
    }
    
    return -1;
}

void *list_fetch_head(struct list *list)
{
    if (!list->head)
        return NULL;
    return list->head->value;
}

void *list_fetch_tail(struct list *list)
{
    if (!list->tail)
        return NULL;
    return list->tail->value;
}

void *list_fetch_index(struct list *list, int index)
{
    struct list_node *node;
    
    if (index < 0)
    {
        index = (-index) - 1;
        
        node = list->tail;
        while (index > 0 && node)
        {
            node = node->prev;
            index--;
        }
    } else {
        node = list->head;
        while (index > 0 && node)
        {
            node = node->next;
            index--;
        }
    }
    
    if (node) {
        return node->value;
    } else {
        return NULL;
    }
}

int list_remove_head(struct list *list, void **value)
{
    struct list_node *node;
    
    if (!list->head)
        return -1;
    node = list->head;
    
    list->head = node->next;
    if (list->head) {
        list->head->prev = NULL;
    } else {
        list->tail = NULL;
    }
    
    if (value) {
        *value = node->value;
    } else {
        list->destroy(node->value);
    }
    
    item_remove(list, node);
    list->size--;
    return 0;
}

int list_remove_tail(struct list *list, void **value)
{
    struct list_node *node;
    
    if (!list->tail)
        return -1;
    node = list->tail;
    
    list->tail = node->prev;
    if (list->tail) {
        list->tail->next = NULL;
    } else {
        list->head = NULL;
    }
    
    if (value) {
        *value = node->value;
    } else {
        list->destroy(node->value);
    }
    
    item_remove(list, node);
    list->size--;
    return 0;
}

int list_remove_index(struct list *list, int index, void **value)
{
    struct list_node *node;
    
    if (index < 0)
    {
        index = (-index) - 1;
        
        node = list->tail;
        while (index > 0 && node)
        {
            node = node->prev;
            index--;
        }
    } else {
        node = list->head;
        while (index > 0 && node)
        {
            node = node->next;
            index--;
        }
    }
    
    if (node) {
        if (value) {
            *value = node->value;
        } else {
            list->destroy(node->value);
        }
        item_remove(list, node);
        return 0;
    }
    
    return -1;
}

void list_destroy(struct list *list)
{
    struct list_node *node, *next;
    
    node = list->head;
    while (node)
    {
        next = node->next;
        list->destroy(node->value);
        free(node); /* destroy is use system free() function */
        node = next;
    }
    
    if (list->cache_size) {
        while (list->cache_used > 0)
            free(list->caches[--list->cache_used]);
        free(list->caches);
    }
    free(list);
}
