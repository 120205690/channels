#include "channel.h"
#include <assert.h>

// Creates a new channel with the provided size and returns it to the caller
channel_t* channel_create(size_t size)
{
    channel_t* channel=(channel_t*)malloc(sizeof(channel_t));
    channel->buffer=buffer_create(size);
    sem_init(&(channel->channel_lock), 0, 1);
    sem_init(&(channel->capacity_left), 0, (unsigned int) size);
    sem_init(&(channel->msgs_available), 0, 0);
    channel->channel_open=size;
    channel->sendhead=NULL;
    channel->receivehead=NULL;
    /* IMPLEMENT THIS */
    return channel;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    sem_wait(&channel->channel_lock);
    if(channel->channel_open==0){
        sem_post(&channel->channel_lock);
        return CLOSED_ERROR;
    }
    sem_post(&channel->channel_lock);
    // sem_getvalue(sem_t *sem, int *val loc);
    sem_wait(&channel->capacity_left);
    sem_wait(&channel->channel_lock);

    if(channel->channel_open==0) {
        sem_post(&channel->capacity_left);
        sem_post(&channel->msgs_available);        
        sem_post(&channel->channel_lock);
        return CLOSED_ERROR;}

    int status = buffer_add(channel->buffer, data);
    

    sem_post(&channel->msgs_available);
    wakeselect(channel, channel->receivehead);
    sem_post(&channel->channel_lock);
    if(status==BUFFER_SUCCESS){
        return SUCCESS;
    }
    return GENERIC_ERROR;

    /* IMPLEMENT THIS */
    // return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    sem_wait(&channel->channel_lock);
    if(channel->channel_open==0){
        sem_post(&channel->channel_lock);
        return CLOSED_ERROR;
    }
    sem_post(&channel->channel_lock);

    sem_wait(&channel->msgs_available);
    sem_wait(&channel->channel_lock);

    if(channel->channel_open==0) {
        // sem_post(&channel->capacity_left);
        // sem_post(&channel->msgs_available);
        // sem_post(&channel->channel_lock);
        sem_post(&channel->capacity_left);
        sem_post(&channel->msgs_available);
        sem_post(&channel->channel_lock);
        return CLOSED_ERROR;}

    int status = buffer_remove(channel->buffer, data);
        
    sem_post(&channel->capacity_left);
    wakeselect(channel, channel->sendhead);
    sem_post(&channel->channel_lock);
    if(status==BUFFER_SUCCESS){
        return SUCCESS;
    }
    return GENERIC_ERROR;
    /* IMPLEMENT THIS */
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    if(channel->channel_open==0){
        return CLOSED_ERROR;
    }

    int sem_status=sem_trywait(&channel->capacity_left);
    if(sem_status==-1){
        return CHANNEL_FULL;
    }

    sem_wait(&channel->channel_lock);

    int status = buffer_add(channel->buffer, data);
    
    sem_post(&channel->msgs_available);
    wakeselect(channel, channel->receivehead);
    sem_post(&channel->channel_lock);
    if(status==BUFFER_SUCCESS){
        return SUCCESS;
    }
    return GENERIC_ERROR;
}

enum channel_status select_non_blocking_send(channel_t* channel, void* data)
{
    if(channel->channel_open==0){
        return CLOSED_ERROR;
    }

    int sem_status=sem_trywait(&channel->capacity_left);
    if(sem_status==-1){
        return CHANNEL_FULL;
    }

    int status = buffer_add(channel->buffer, data);
    
    sem_post(&channel->msgs_available);
    wakeselect(channel, channel->receivehead);
    if(status==BUFFER_SUCCESS){
        return SUCCESS;
    }
    return GENERIC_ERROR;
}
enum channel_status select_non_blocking_receive(channel_t* channel, void** data)
{
    if(channel->channel_open==0){
        return CLOSED_ERROR;
    }

    int sem_status = sem_trywait(&channel->msgs_available);
    if(sem_status==-1){
        return CHANNEL_FULL;
    }

    int status = buffer_remove(channel->buffer, data);
    
    sem_post(&channel->capacity_left);
    wakeselect(channel, channel->sendhead);
    if(status==BUFFER_SUCCESS){
        return SUCCESS;
    }
    return GENERIC_ERROR;
}
// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    if(channel->channel_open==0){
        return CLOSED_ERROR;
    }
    
    int sem_status = sem_trywait(&channel->msgs_available);
    if(sem_status==-1){
        return CHANNEL_EMPTY;
    }
    sem_wait(&channel->channel_lock);

    int status = buffer_remove(channel->buffer, data);
    
    sem_post(&channel->capacity_left);
    wakeselect(channel, channel->sendhead);
    sem_post(&channel->channel_lock);
    if(status==BUFFER_SUCCESS){
        return SUCCESS;
    }
    return GENERIC_ERROR;
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GENERIC_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    /* IMPLEMENT THIS */
    sem_wait(&channel->channel_lock);
    size_t size=channel->channel_open;
    if(channel->channel_open==0) {
        sem_post(&channel->channel_lock);
        return CLOSED_ERROR;}
    channel->channel_open=0;
    
    for(int i=0;i<size;i++){
        sem_post(&channel->capacity_left);
        sem_post(&channel->msgs_available);
    }
    wakeall(channel, channel->sendhead);
    wakeall(channel, channel->receivehead);
    sem_post(&channel->channel_lock);
    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GENERIC_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    if(channel->channel_open!=0) return DESTROY_ERROR;
    buffer_free(channel->buffer);
    int a=sem_destroy(&(channel->channel_lock));
    int b=sem_destroy(&(channel->capacity_left));
    int c=sem_destroy(&(channel->msgs_available));
    free(channel);
    /* IMPLEMENT THIS */
    if (a || b ||c) return GENERIC_ERROR;
    return SUCCESS;
    
}
void destroy_select(sem_t* select_fuse, sem_t* select_waitlock)
{
    sem_destroy(select_fuse);
    sem_destroy(select_waitlock);
    free(select_fuse);
    free(select_waitlock);
}
// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    sem_t* select_waitlock = (sem_t*)malloc(sizeof(sem_t));
    sem_t* select_fuse = (sem_t*)malloc(sizeof(sem_t));
    sem_init(select_waitlock, 0, 0);
    sem_init(select_fuse, 0, 1);

    Node* channel_head = NULL;
    enum channel_status status; enum direction dir;
    int iter=0;
    while(1){
        for(size_t i=0; i<channel_count; i++){
            channel_t* channel=channel_list[i].channel;
            void* data=channel_list[i].data;
            dir=channel_list[i].dir;
            
            sem_wait(&channel->channel_lock);
            if(channel->channel_open==0){
                removeallnodes(channel_list, channel_count, select_fuse, channel, 1);
                sem_post(&channel->channel_lock);
                removeallnodes(channel_list, channel_count, select_fuse, channel, 0);
                *selected_index=i;
                
                destroy_select(select_fuse, select_waitlock);
                return CLOSED_ERROR;
            }

            if(dir==SEND){
                channel_head = &channel->sendhead;
                status= select_non_blocking_send(channel, data);
            }
            else{
                channel_head = &channel->receivehead;
                status= select_non_blocking_receive(channel, &channel_list[i].data);
            }
            
            Node channel_selectnode=NULL;
            if(*channel_head!=NULL){
                channel_selectnode=ispresent(*channel_head, select_fuse);}

            if(status==SUCCESS){
                removeallnodes(channel_list, channel_count, select_fuse, channel, 1);
                sem_post(&channel->channel_lock);
                removeallnodes(channel_list, channel_count, select_fuse, channel, 0);
                *selected_index=i;
                
                destroy_select(select_fuse, select_waitlock);
                return SUCCESS;
            }
            else if(status==CLOSED_ERROR){
                removeallnodes(channel_list, channel_count, select_fuse, channel, 1);
                sem_post(&channel->channel_lock);
                removeallnodes(channel_list, channel_count, select_fuse, channel, 0);
                *selected_index=i;
                
                destroy_select(select_fuse, select_waitlock);
                return CLOSED_ERROR;
            }
            else{
                if(channel_selectnode==NULL){
                    Node head=addnode(*channel_head, select_fuse, select_waitlock);
                    *channel_head=head;
                }
            }
            sem_post(&channel->channel_lock);
        }
        iter++;
        sem_wait(select_waitlock);
        sem_post(select_fuse);
        //Check for closed channels
        for(size_t i=0; i<channel_count; i++){
            channel_t* channel=channel_list[i].channel;
            sem_wait(&channel->channel_lock);
            if(channel->channel_open==0){
                removeallnodes(channel_list, channel_count, select_fuse, channel, 1);
                sem_post(&channel->channel_lock);
                removeallnodes(channel_list, channel_count, select_fuse, channel, 0);
                *selected_index=i;
                destroy_select(select_fuse, select_waitlock);
                return CLOSED_ERROR;
            }
            sem_post(&channel->channel_lock);
        }
    }
}

void removeallnodes(select_t* channel_list, size_t channel_count, sem_t* select_fuse, channel_t* s_channel, int same){
    
    for(int i=0; i<channel_count; i++){
                        
        channel_t* channel=channel_list[i].channel;
        enum direction dir=channel_list[i].dir;

        if(same==1 && channel!=s_channel) continue;
        if(same==0 && channel==s_channel) continue;
        if((same==0 && channel!=s_channel)||same==2) sem_wait(&channel->channel_lock);
        
        Node* channel_head;
        if(dir==SEND){
            channel_head = &channel->sendhead;
        }
        else{
            channel_head = &channel->receivehead;
        }
        
        Node head=removenode(*channel_head, select_fuse);
        *channel_head=head;
        
        if((same==0 && channel!=s_channel)||same==2) sem_post(&channel->channel_lock);
    }
    return;
}

void wakeselect(channel_t* channel, Node head){
    
    if(head==NULL) return;
    while(head!=NULL){
        int fuse_status=sem_trywait(head->fuse);
        if(fuse_status!=-1){
        sem_post(head->waitlock);
            return;
        }
        head=head->next;
    }
    return;
}

void wakeall(channel_t* channel, Node head){
    Node ptr=head;
    if(head==NULL) return;
    while(ptr!=NULL){
        sem_post(ptr->waitlock);
        sem_post(ptr->fuse);
        ptr=ptr->next;
    }
    return;
}

Node addnode(Node head, sem_t* fuse, sem_t* waitlock){
    if(head==NULL){
        head =(Node)malloc(sizeof(struct node));
        head->fuse=fuse;
        head->waitlock=waitlock;
        head->next=NULL;
        return head;
    }
    Node ptr=head;
    Node prev;
    while(ptr!=NULL){
        prev=ptr;
        ptr=ptr->next;
    }
    
    ptr=(Node)malloc(sizeof(struct node));
    ptr->fuse=fuse;
    ptr->waitlock=waitlock;
    ptr->next=NULL;
    prev->next=ptr;
    return head;
}
Node removenode(Node head, sem_t* targetfuse){
    Node ptr=head;
    
    if(ptr==NULL){
        assert("Removal from Empty List");
    }
    else if(head->fuse == targetfuse){
        head=head->next;
        free(ptr);
    }
    else{
        Node prev=head;
        while(ptr!= NULL){
            if(ptr->fuse == targetfuse){
                break;
            } 
            prev=ptr;
            ptr=ptr->next;       
            
        }
        if(ptr==NULL){
            assert("Node to be removed not found");
        }
        else{
            prev->next=ptr->next;
            free(ptr);
        }
    }
    return head;
}
Node ispresent(Node head, sem_t* fuse){
    Node ptr=head;
    while(ptr!=NULL){
        if(ptr->fuse==fuse)
            return ptr;
        ptr=ptr->next;
    }
    return NULL;
}
void printlist(Node x){
    if(x!=NULL){
        printf("##########\n");
        while(x!=NULL){
            printf("%p, %p, %p, %p\n", x, x->fuse, x->waitlock,x->next);
        }
    }
    else    printf("NULL List");
}