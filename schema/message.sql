-- Table: public.message

-- DROP TABLE IF EXISTS public.message;

CREATE TABLE IF NOT EXISTS public.message
(
    id integer NOT NULL DEFAULT nextval('message_id_seq'::regclass),
    topic character varying(255) COLLATE pg_catalog."default",
    publication_timestamp timestamp without time zone,
    insert_timestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    data_id character varying(255) COLLATE pg_catalog."default",
    canonical_url character varying(255) COLLATE pg_catalog."default",
    wigos_station_identifier character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT message_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.message
    OWNER to postgres;