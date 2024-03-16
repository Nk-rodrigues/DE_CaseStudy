CREATE TABLE user_requests (
    ip_address VARCHAR2(4000),
    rfc_id VARCHAR2(4000),
    user_id VARCHAR2(4000),
    timestamp VARCHAR2(4000),
    request_method VARCHAR2(4000),
    request_resource VARCHAR2(4000),
    request_protocol VARCHAR2(4000),
    status_code VARCHAR2(4000),
    object_size VARCHAR2(4000),
    url VARCHAR2(4000),
    browser VARCHAR2(4000),
    user_time_key VARCHAR2(64),
    CONSTRAINT pk_user_time_key PRIMARY KEY (user_time_key)
);