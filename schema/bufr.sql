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
    wigosidentifierseries integer,
    wigosissuerofidentifier character varying(255) COLLATE pg_catalog."default",
    wigosissuenumber integer,
    wigoslocalidentifiercharacter character varying(255) COLLATE pg_catalog."default",
    blocknumber integer,
    stationnumber integer,
    latitude double precision,
    longitude double precision,
    elevation double precision,
    heightofbarometerabovemeansealevel double precision,
    unexpanded_descriptors character varying(255) COLLATE pg_catalog."default",
    raw_data jsonb,
    CONSTRAINT bufr_pkey PRIMARY KEY (id),
    CONSTRAINT bufr_message_id_fkey FOREIGN KEY (message_id)
        REFERENCES public.message (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.bufr
    OWNER to postgres;
