<?php
namespace YiiElasticSearch\Enum;

class SearchType
{
    const QUERY_THEN_FETCH = 'query_then_fetch';
    const DFS_QUERY_THEN_FETCH = 'dfs_query_then_fetch';
    const COUNT = 'count';
    const SCAN = 'scan';
}
