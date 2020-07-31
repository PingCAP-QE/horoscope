# Index selection fuzz

Horoscope fuzzes the index selection by adding new random indexes and generating SQL queries.

## 1. Random add indexes

First, horoscope connects to the target database, reads schema information, and decides which indexes(in current implementation, horo selects column combinations randomly) to add, then saves the DDL statements on local `benchmark/dyn/indexes` dir.

* `benchmark/dyn/indexes/add-indexes.sql`

```sql
ALTER TABLE `cast_info` ADD INDEX `CAST_INFO_person_role_id_person_id_IDX` (person_role_id,person_id);
ALTER TABLE `cast_info` ADD INDEX `CAST_INFO_id_person_id_IDX` (id,person_id);
ALTER TABLE `cast_info` ADD INDEX `CAST_INFO_nr_order_person_role_id_IDX` (nr_order,person_role_id);
```

* `benchmark/dyn/indexes/clean-indexes.sql`

```sql
ALTER TABLE `cast_info` DROP INDEX `CAST_INFO_person_role_id_person_id_IDX`;
ALTER TABLE `cast_info` DROP INDEX `CAST_INFO_id_person_id_IDX`;
ALTER TABLE `cast_info` DROP INDEX `CAST_INFO_nr_order_person_role_id_IDX`;
```

## 2. Apply new indexes

After `benchmark/dyn/indexes/add-indexes.sql` is generated, we need to use `horo index add` to apply these new indexes.

## 3. Generate SQL queries

Horoscope decides which tables need to join together and read one random row by each table.

Then, it groups the where clause parts by table names, each group may contain several selections which are connected by `AND` or `OR`.

Each selection uses the selected row to instantiate the filtered value, we combine `<=>`, `<` and `>` on selections.

Finally, `AND` is used to join each group and the `ORDER BY` and `LIMIT` expression is inserted in the end.

```sql
SELECT *
FROM info_type,
     kind_type,
     link_type
WHERE ((info_type.id <=> 49
        OR info_type.id < 49)
       AND (info_type.info <=> 'LD spaciality'
            OR info_type.info > 'LD spaciality'))
  AND ((kind_type.id <=> 6
        OR kind_type.id < 6)
       OR (kind_type.kind <=> 'video game'
           OR kind_type.kind > 'video game'))
  AND ((link_type.id <=> 6
        OR link_type.id < 6)
       OR (link_type.link <=> 'referenced in'
           OR link_type.link < 'referenced in'))
ORDER BY link_type.id,
         kind_type.kind,
         info_type.id
LIMIT 100;
```
