#enable global index route
alter proxyconfig set enable_index_route='true';
alter proxyconfig set enable_reroute='true';
alter proxyconfig set enable_ob_protocol_v2='true';

#obproxy server keepalive

alter proxyconfig set sock_option_flag_out = 3;  
alter proxyconfig set server_tcp_keepidle = 5;  
alter proxyconfig set server_tcp_keepintvl = 5;  
alter proxyconfig set server_tcp_keepcnt = 2;  
alter proxyconfig set server_tcp_user_timeout = 10; 

alter proxyconfig set client_sock_option_flag_out = 2;
alter proxyconfig set client_tcp_keepidle = 5;
alter proxyconfig set client_tcp_keepintvl = 5;  
alter proxyconfig set client_tcp_keepcnt = 2; 
alter proxyconfig set client_tcp_user_timeout = 10;

#set memory
alter proxyconfig set proxy_mem_limited ='4g';
alter proxyconfig set enable_cached_server='false';
