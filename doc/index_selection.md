# Index selection fuzz

Horoscope fuzzes the index selection by adding new random indexes and generating SQL queries.

## 1. Random add indexes

First, horoscope connects to the target database, reads schema information, and decides which indexes(in current implementation, horo selects column combinations randomly) to add, then saves the DDL statements in `indexes` directory of workload.

Use `horo index new` to generate DDLs:

* `indexes/add-indexes.sql`

```sql
ALTER TABLE `cast_info` ADD INDEX `CAST_INFO_person_role_id_person_id_IDX` (person_role_id,person_id);
ALTER TABLE `cast_info` ADD INDEX `CAST_INFO_id_person_id_IDX` (id,person_id);
ALTER TABLE `cast_info` ADD INDEX `CAST_INFO_nr_order_person_role_id_IDX` (nr_order,person_role_id);
```

* `indexes/clean-indexes.sql`

```sql
ALTER TABLE `cast_info` DROP INDEX `CAST_INFO_person_role_id_person_id_IDX`;
ALTER TABLE `cast_info` DROP INDEX `CAST_INFO_id_person_id_IDX`;
ALTER TABLE `cast_info` DROP INDEX `CAST_INFO_nr_order_person_role_id_IDX`;
```

## 2. Apply new indexes

After `indexes/add-indexes.sql` is generated, we need to use `horo index add` to apply these new indexes.

## 3. Generate SQL queries

Horoscope decides which tables need to join together and read one random row by each table.

Then, it groups the where clause parts by table names, each group may contain several selections which are connected by `AND` or `OR`.

Each selection uses the selected row to instantiate the filtered value, we support all [range conditions](https://dev.mysql.com/doc/refman/8.0/en/range-optimization.html).

Finally, `AND` is used to join each group and the `ORDER BY` and `LIMIT` expression is inserted in the end.

```sql
SELECT *
FROM (aka_title JOIN title ON aka_title.movie_id=title.id)
WHERE aka_title.id IN ("388200",
                       "482320",
                       "72142",
                       "460135",
                       "446635",
                       "499473")
  AND aka_title.movie_id>"4421863"
  AND aka_title.title!="Apeiron III: The Tormented"
  AND aka_title.imdb_index<=>NULL
  AND aka_title.kind_id<="1"
  OR aka_title.production_year<=>NULL
  AND aka_title.phonetic_code<=>"B563"
  AND aka_title.episode_of_id IS NULL
  AND aka_title.season_nr IS NULL
  OR aka_title.episode_nr IS NULL
  AND aka_title.md5sum IS NOT NULL
  AND title.id<=>"4252321"
  AND title.title>="Rock Band"
  AND title.imdb_index<=>NULL
  AND title.kind_id>="1"
  AND title.production_year<=>NULL
  AND title.imdb_id<=>NULL
  AND title.phonetic_code<="R2153"
  OR title.episode_of_id<=>NULL
  AND title.season_nr IS NULL
  OR title.episode_nr IS NULL
  AND title.series_years<=>NULL
  AND title.md5sum BETWEEN "0c9e0c716b81fe6d8c3ef03cca15ff85" AND "c89f9fa35c30357690827ceadf06ed4f"
ORDER BY aka_title.id,
         title.id
LIMIT 100
```
