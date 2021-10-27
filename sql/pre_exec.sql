CREATE TABLE IF NOT EXISTS  mobile_events (
	anonymous_id varchar(36) NULL,
	business_zoned varchar(7) NULL,
	context_app_version varchar(6) NULL,
	context_device_id varchar(36) NULL,
	context_os_name varchar(7) NULL,
	event_name varchar(51) NULL,
	received_at timestamp NOT NULL,
	created_at timestamp NOT NULL
);








