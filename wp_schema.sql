
--
-- Table structure for table "redirect"
--
CREATE SCHEMA IF NOT EXISTS wp;
DROP TABLE IF EXISTS wp.redirect;
CREATE TABLE wp.redirect (
  "rd_from" integer NOT NULL DEFAULT '0',
  "rd_namespace" integer NOT NULL DEFAULT '0',
  "rd_title" text NOT NULL DEFAULT '',
  "rd_interwiki" text DEFAULT NULL,
  "rd_fragment" text DEFAULT NULL
 );


DROP TABLE IF EXISTS wp.page;
CREATE TABLE wp.page (
  "page_id" integer NOT NULL ,
  "page_namespace" integer NOT NULL DEFAULT '0',
  "page_title" TEXT NOT NULL DEFAULT '',
  "page_restrictions" TEXT NOT NULL,
  "page_is_redirect" integer NOT NULL DEFAULT '0',
  "page_is_new" integer NOT NULL DEFAULT '0',
  "page_random" float NOT NULL DEFAULT '0',
  "page_touched" TEXT NOT NULL DEFAULT '',
  "page_links_updated" TEXT DEFAULT NULL,
  "page_latest" integer NOT NULL DEFAULT '0',
  "page_len" integer NOT NULL DEFAULT '0',
  "page_content_model" TEXT DEFAULT NULL,
  "page_lang" TEXT DEFAULT NULL
);


DROP TABLE IF EXISTS wp.geo_tags;
CREATE TABLE wp.geo_tags (
  "gt_id" integer NOT NULL,
  "gt_page_id" integer NOT NULL,
  "gt_globe" TEXT NOT NULL,
  "gt_primary" integer NOT NULL,
  "gt_lat" float DEFAULT NULL,
  "gt_lon" float DEFAULT NULL,
  "gt_dim" integer DEFAULT NULL,
  "gt_type" TEXT DEFAULT NULL,
  "gt_name" TEXT DEFAULT NULL,
  "gt_country" TEXT DEFAULT NULL,
  "gt_region" TEXT DEFAULT NULL
);

CREATE TABLE wp.pagelinks (
  "pl_from" integer,
  "pl_namespace" integer,
  "pl_title" TEXT,
  "pl_from_namespace" integer
);

DROP TABLE IF EXISTS wp.page_props;
CREATE TABLE "wp.page_props" (
      "pp_page" integer,
      "pp_propname" TEXT,
      "pp_value" TEXT,
      "pp_sortkey" float DEFAULT NULL
);
