alter proxyconfig set server_detect_fail_threshold=300;
#alter proxyconfig set slow_proxy_process_time_threshold='500ms';

alter proxyconfig set client_max_connections=65535;
alter proxyconfig set proxy_mem_limited='100G';
alter proxyconfig set enable_ob_protocol_v2=True;
alter proxyconfig set enable_reroute=True;
alter proxyconfig set enable_index_route=True;

#
#alter proxyconfig set sock_option_flag_out=2;
#alter proxyconfig set server_tcp_keepidle=5 ;
#alter proxyconfig set server_tcp_keepintvl=5;
#alter proxyconfig set server_tcp_user_timeout=5;
