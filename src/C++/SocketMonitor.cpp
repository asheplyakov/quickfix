/****************************************************************************
** Copyright (c) 2001-2014
**
** This file is part of the QuickFIX FIX Engine
**
** This file may be distributed under the terms of the quickfixengine.org
** license as defined by quickfixengine.org and appearing in the file
** LICENSE included in the packaging of this file.
**
** This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING THE
** WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
**
** See http://www.quickfixengine.org/LICENSE for licensing information.
**
** Contact ask@quickfixengine.org if any conditions of this licensing are
** not clear to you.
**
****************************************************************************/

#ifdef _MSC_VER
#include "stdafx.h"
#else
#include "config.h"
#endif
#ifdef __linux__
#define USE_EPOLL
#else
#define USE_SELECT
#endif

#include "SocketMonitor.h"
#include "Utility.h"
#ifdef USE_EPOLL
#include <sys/epoll.h>
#endif
#include <exception>
#include <set>
#include <algorithm>
#include <iostream>

namespace FIX
{
SocketMonitor::SocketMonitor( int timeout )
: m_timeout( timeout )
{
  socket_init();

#ifdef USE_EPOLL
  m_epfd = epoll_create1(EPOLL_CLOEXEC);
  if (m_epfd < 0) {
      throw std::runtime_error("epoll_create1");
  }
  m_signal = m_interrupt = -1;
#else
  std::pair<socket_handle, socket_handle> sockets = socket_createpair();
  m_signal = sockets.first;
  m_interrupt = sockets.second;
  socket_setnonblock( m_signal );
  socket_setnonblock( m_interrupt );
  m_readSockets.insert( m_interrupt );
#endif

  m_timeval.tv_sec = 0;
  m_timeval.tv_usec = 0;
#ifndef SELECT_DECREMENTS_TIME
  m_ticks = clock();
#endif
}

SocketMonitor::~SocketMonitor()
{
  Sockets::iterator i;
  for ( i = m_readSockets.begin(); i != m_readSockets.end(); ++i ) {
    socket_close( *i );
  }

#ifdef USE_EPOLL
  close(m_epfd);
#else
  socket_close( m_signal );
#endif
  socket_term();
}

size_t SocketMonitor::numSockets() const
{
#ifdef USE_EPOLL
  return m_readSockets.size();
#else
  return m_readSockets.size() - 1;
#endif
}

bool SocketMonitor::addConnect(socket_handle s )
{
  socket_setnonblock( s );
  Sockets::iterator i = m_connectSockets.find( s );
  if( i != m_connectSockets.end() ) return false;

#ifdef USE_EPOLL
  epoll_event event;
  event.events = EPOLLOUT;
  event.data.fd = s;
  int op = EPOLL_CTL_ADD;
  if (m_writeSockets.find(s) != m_writeSockets.end()) {
      op = EPOLL_CTL_MOD;
  }
  if (m_readSockets.find(s) != m_readSockets.end()) {
      op = EPOLL_CTL_MOD;
      event.events |= EPOLLIN;
  }
  if (epoll_ctl(m_epfd, op, s, &event) < 0) {
      throw std::runtime_error("SocketMonitor::addConnect: epoll_ctl");
  }
#endif
  m_connectSockets.insert( s );
  return true;
}

bool SocketMonitor::addRead(socket_handle s )
{
  socket_setnonblock( s );
  Sockets::iterator i = m_readSockets.find( s );
  if( i != m_readSockets.end() ) return false;

#ifdef USE_EPOLL
  epoll_event event;
  event.events = EPOLLIN;
  event.data.fd = s;
  if (epoll_ctl(m_epfd, EPOLL_CTL_ADD, s, &event) < 0) {
      throw std::runtime_error("SocketMonitor::addRead: epoll_ctl");
  }
#endif
  m_readSockets.insert( s );
  return true;
}

bool SocketMonitor::addWrite(socket_handle s )
{
  if( m_readSockets.find(s) == m_readSockets.end() )
    return false;

  socket_setnonblock( s );
  Sockets::iterator i = m_writeSockets.find( s );
  if( i != m_writeSockets.end() ) return false;

#ifdef USE_EPOLL
  epoll_event event;
  event.events = EPOLLIN|EPOLLOUT;
  if (epoll_ctl(m_epfd, EPOLL_CTL_MOD, s, &event) < 0) {
      throw std::runtime_error("SocketMonitor::addWrite: epoll_ctl");
  }
#endif
  m_writeSockets.insert( s );
  return true;
}

bool SocketMonitor::drop(socket_handle s )
{
  Sockets::iterator i = m_readSockets.find( s );
  Sockets::iterator j = m_writeSockets.find( s );
  Sockets::iterator k = m_connectSockets.find( s );
#ifdef USE_EPOLL
  epoll_ctl(m_epfd, EPOLL_CTL_DEL, s, NULL);
#endif

  if ( i != m_readSockets.end() || 
       j != m_writeSockets.end() ||
       k != m_connectSockets.end() )
  {
    socket_close( s );
    m_readSockets.erase( s );
    m_writeSockets.erase( s );
    m_connectSockets.erase( s );
    m_dropped.push( s );
    return true;
  }
  return false;
}

inline timeval* SocketMonitor::getTimeval( bool poll, double timeout )
{
  if ( poll )
  {
    m_timeval.tv_sec = 0;
    m_timeval.tv_usec = 0;
    return &m_timeval;
  }

  timeout = m_timeout;

  if ( !timeout )
    return 0;
#ifdef SELECT_MODIFIES_TIMEVAL
  if ( !m_timeval.tv_sec && !m_timeval.tv_usec && timeout )
    m_timeval.tv_sec = timeout;
  return &m_timeval;
#else
  double elapsed = ( double ) ( clock() - m_ticks ) / ( double ) CLOCKS_PER_SEC;
  if ( elapsed >= timeout || elapsed == 0.0 )
  {
    m_ticks = clock();
    m_timeval.tv_sec = 0;
    m_timeval.tv_usec = (long)(timeout * 1000000);
  }
  else
  {
    m_timeval.tv_sec = 0;
    m_timeval.tv_usec = (long)( ( timeout - elapsed ) * 1000000 );
  }
  return &m_timeval;
#endif
}

bool SocketMonitor::sleepIfEmpty( bool poll )
{
  if( poll )
    return false;

  if ( m_readSockets.empty() && 
       m_writeSockets.empty() &&
       m_connectSockets.empty() )
  {
    process_sleep( m_timeout );
    return true;
  }
  else
    return false;
}

void SocketMonitor::signal(socket_handle socket )
{
#ifdef USE_EPOLL
  addWrite(socket);
#else
  socket_send( m_signal, (char*)&socket, sizeof(socket) );
#endif
}

void SocketMonitor::unsignal(socket_handle s )
{
  Sockets::iterator i = m_writeSockets.find( s );
  if( i == m_writeSockets.end() ) return;

#ifdef USE_EPOLL
  epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.data.fd = s;
  if (m_connectSockets.find(s) != m_connectSockets.end()) {
      ev.events |= EPOLLIN;
  }
  if (m_readSockets.find(s) != m_readSockets.end()) {
      ev.events |= EPOLLIN;
  }
  if (ev.events) {
      if (epoll_ctl(m_epfd, EPOLL_CTL_MOD, s, &ev) < 0) {
        throw std::runtime_error("SocketMonitor::unsignal: EPOLL_CTL_MOD");
      }
  } else {
     // XXX: should I bail out on failure?
     epoll_ctl(m_epfd, EPOLL_CTL_DEL, s, NULL);
  }
#endif
  m_writeSockets.erase( s );
}

int SocketMonitor::select(fd_set *readSet, fd_set *writeSet, fd_set *exceptSet, timeval *timeout)
{
#ifdef USE_EPOLL
  int result = 0;
  epoll_event events[FD_SETSIZE];
  int timeoutMsec = timeout ? (1000*timeout->tv_sec + timeout->tv_usec/1000) : -1;
  do {
      result = epoll_wait(m_epfd, events, sizeof(events)/sizeof(events[0]), timeoutMsec);
      if (result < 0) {
          if (errno == EINTR) {
              continue;
          } else {
              // TODO: log the error
          }
      }
  } while (result < 0);
  if (result > 0) {
      for (int i = 0; i < result; i++) {
          if (events[i].events & EPOLLERR) {
              FD_SET(events[i].data.fd, exceptSet);
          }
          if (events[i].events & EPOLLOUT) {
              FD_SET(events[i].data.fd, writeSet);
          }
          if (events[i].events & EPOLLIN) {
              FD_SET(events[i].data.fd, readSet);
          }
      }
  }
  return result;
#else
  buildSet(m_readSockets, *readSet);
  buildSet(m_connectSockets, *writeSet);
  buildSet(m_writeSockets, *writeSet);
  buildSet(m_connectSockets, *exceptSet);
  int result = ::select(FD_SETSIZE, readSet, writeSet, exceptSet, timeout);
  return result;
#endif
}

void SocketMonitor::block( Strategy& strategy, bool poll, double timeout )
{
  while ( m_dropped.size() )
  {
    strategy.onError( *this, m_dropped.front() );
    m_dropped.pop();
    if ( m_dropped.size() == 0 )
      return ;
  }

  fd_set readSet;
  FD_ZERO( &readSet );
  fd_set writeSet;
  FD_ZERO( &writeSet );
  fd_set exceptSet;
  FD_ZERO( &exceptSet );

  if ( sleepIfEmpty(poll) )
  {
    strategy.onTimeout( *this );
    return;
  }

  int result = this->select(&readSet, &writeSet, &exceptSet, getTimeval(poll, timeout));
  if ( result == 0 )
  {
    strategy.onTimeout( *this );
    return;
  }
  else if ( result > 0 )
  {
    processExceptSet( strategy, exceptSet );
    processWriteSet( strategy, writeSet );
    processReadSet( strategy, readSet );
  }
  else
  {
    strategy.onError( *this );
  }
}

void SocketMonitor::processReadSet( Strategy& strategy, fd_set& readSet )
{
#ifdef _MSC_VER
  for ( unsigned i = 0; i < readSet.fd_count; ++i )
  {
    socket_handle s = readSet.fd_array[ i ];
    if( s == m_interrupt )
    {
      socket_handle socket = 0;
      socket_recv( s, (char*)&socket, sizeof(socket) );
      addWrite( socket );
    }
    else
    {
      strategy.onEvent( *this, s );
    }
  }
#else
    Sockets::iterator i;
    Sockets sockets = m_readSockets;
    for ( i = sockets.begin(); i != sockets.end(); ++i )
    {
      socket_handle s = *i;
      if ( !FD_ISSET( *i, &readSet ) )
        continue;
      if( s == m_interrupt )
      {
#ifndef USE_EPOLL
        socket_handle socket = 0;
        socket_recv( s, (char*)&socket, sizeof(socket) );
        addWrite( socket );
#endif
      }
      else
      {
        strategy.onEvent( *this, s );
      }
    }
#endif
}

void SocketMonitor::processWriteSet( Strategy& strategy, fd_set& writeSet )
{
#ifdef _MSC_VER
  for ( unsigned i = 0; i < writeSet.fd_count; ++i )
  {
    socket_handle s = writeSet.fd_array[ i ];
    if( m_connectSockets.find(s) != m_connectSockets.end() )
    {
      m_connectSockets.erase( s );
      m_readSockets.insert( s );
      strategy.onConnect( *this, s );
    }
    else
    {
      strategy.onWrite( *this, s );
    }
  }
#else
  Sockets::iterator i;
  Sockets sockets = m_connectSockets;
  for( i = sockets.begin(); i != sockets.end(); ++i )
  {
    socket_handle s = *i;
    if ( !FD_ISSET( *i, &writeSet ) )
      continue;
    m_connectSockets.erase( s );
    m_readSockets.insert( s );
#ifdef USE_EPOLL
    epoll_event event;
    event.data.fd = s;
    event.events = EPOLLIN;
    if (m_writeSockets.find(s) != m_writeSockets.end()) {
        event.events |= EPOLLOUT;
    }
    if (epoll_ctl(m_epfd, EPOLL_CTL_MOD, s, &event) < 0) {
        throw std::runtime_error("SocketMonitor::processWriteSet: epoll_ctl");
    }
#endif
    strategy.onConnect( *this, s );
  }

  sockets = m_writeSockets;
  for( i = sockets.begin(); i != sockets.end(); ++i )
  {
    socket_handle s = *i;
    if ( !FD_ISSET( *i, &writeSet ) )
      continue;
    strategy.onWrite( *this, s );
  }
#endif
}

void SocketMonitor::processExceptSet( Strategy& strategy, fd_set& exceptSet )
{
#ifdef _MSC_VER
  for ( unsigned i = 0; i < exceptSet.fd_count; ++i )
  {
    socket_handle s = exceptSet.fd_array[ i ];
    strategy.onError( *this, s );
  }
#else
    Sockets::iterator i;
    Sockets sockets = m_connectSockets;
    for ( i = sockets.begin(); i != sockets.end(); ++i )
    {
      socket_handle s = *i;
      if ( !FD_ISSET( *i, &exceptSet ) )
        continue;
      strategy.onError( *this, s );
    }
#endif
}

void SocketMonitor::buildSet( const Sockets& sockets, fd_set& watchSet )
{
  Sockets::const_iterator iter;
  for ( iter = sockets.begin(); iter != sockets.end(); ++iter ) {
    FD_SET( *iter, &watchSet );
  }
}
}
