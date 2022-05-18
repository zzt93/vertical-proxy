###### Based on [shardingsphere](https://github.com/apache/shardingsphere)

Using SQL to access data from unified entry which backed by multiple different datasource.

Frontend: 
- MySQL

Backend:
- MySQL driver compatible
    - MySQL
    - TiDB
- ClickHouse
- Presto 

---

###### Limitation

- Performance
- Only one-to-one
- Not support group by
- Only order by `id`
- Filter by any other column except `id` will return record no such column
  - because join between different data source can't push down