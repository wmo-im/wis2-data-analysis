-- Table: public.bufr

-- DROP TABLE IF EXISTS public.bufr;

CREATE TABLE IF NOT EXISTS public.bufr
(
    id integer NOT NULL DEFAULT nextval('bufr_id_seq'::regclass),
    message_id integer,
    year integer,
    month integer,
    day integer,
    hour integer,
    minute integer,
    wigos_identifier_series_number integer,
    wigos_identifier_issuer character varying(255) COLLATE pg_catalog."default",
    wigos_identifier_issue_number integer,
    wigos_identifier_local_identifier character varying(255) COLLATE pg_catalog."default",
    block_number integer,
    station_number integer,
    latitude double precision,
    longitude double precision,
    station_elevation double precision,
    barometer_height_above_sealevel double precision,
    unexpanded_descriptors character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT bufr_pkey PRIMARY KEY (id),
    CONSTRAINT bufr_message_id_fkey FOREIGN KEY (message_id)
        REFERENCES public.message (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.bufr
    OWNER to postgres;
