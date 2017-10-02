<?php

namespace YiiElasticSearch;

use ArrayObject;
use Exception;

/**
 * Represents a search request to elasticsearch
 *
 * This class is mainly an OO container for search parameters.
 * @see https://www.elastic.co/guide/en/elasticsearch/client/php-api/2.0/ElasticsearchPHP_Endpoints.html#Elasticsearch_Clientsearch_search
 * for available parameters.
 *
 * You can set arbitrary properties:
 *
 *      $search = new YiiElasticSearch\Search;
 *      $search->query = array(
 *          'match_all' => array(),
 *      );
 *      $search->filter = array(
 *          'term' => array('user'=>'foo'),
 *      );
 *
 * @author Charles Pick <charles.pick@gmail.com>
 * @licence MIT
 * @package YiiElasticSearch
 */
class Search extends ArrayObject
{
    /**
     * @var string|array A list of index names to search; use `_all` or
     */
    public $index;
    /**
     * document types to search. Empty searching in `all`
     * @var string|array
     */
    public $type;
    /**
     * @var string The analyzer to use for the query string
     */
    public $analyzer;
    /**
     * @var boolean Specify whether wildcard and prefix queries should be analyzed
     */
    public $analyze_wildcard = false;
    /**
     * @var string The default operator for query string query (AND or OR) (AND,OR)
     */
    public $default_operator = Enum\Operator::OP_OR;
    /**
     * @var string The field to use as default where no field prefix is given in the query string
     */
    public $df;
    /**
     * @var booolean Specify whether to return detailed information about score computation as part of a hit
     */
    public $explain = false;
    /**
     * A list of fields to return as part of a hit
     * @var array
     */
    public $fields = [];
    /**
     * list of fields to return as the field data representation of a field for each hit
     * @var array
     */
    public $fielddata_fields = [];
    /**
     * @var integer Starting offset
     */
    public $from = 0;
    /**
     * @var  When performed on multiple indices, allows to ignore `missing` ones
     */
    public $ignore_indices = 0;
    /**
     * A list of index boosts
     * @var array
     */
    public $indices_boost = [];
    /**
     * Enable query cache for this request
     * @var boolean
     */
    public $query_cache = false;
    /**
    * Whether specified concrete indices should be ignored when unavailable (missing or closed)
    * @var boolean
    */
    public $ignore_unavailable = true;
    /**
    * Whether to ignore if a wildcard indices expression resolves into no concrete indices.
    * (This includes `_all` string or when no indices have been specified)
    * @var boolean
    */
    public $allow_no_indices;
    /**
    * Whether to expand wildcard expression to concrete indices that are open, closed or both.(default: open)
    * @var string
    */
    public $expand_wildcards = Enum\Wildcard::EXPAND_OPEN;
    /**
     * Specify whether format-based query failures (such as providing text to a numeric field) should be ignored
     * @var boolean
     */
    public $lenient;
    /**
     * Specify whether query terms should be lowercased
     * @var booleab
     */
    public $lowercase_expanded_terms;
    /**
     * Specify the node or shard the operation should be performed on (default: random)
     * @var string
     */
    public $preference = 'random';
    /**
     * Query in the Lucene query string syntax
     * @var string
     */
    public $q;
    /**
     * A list of specific routing values
     * @var array
     */
    public $routing = [];
    /**
     * Specify how long a consistent (timestamp) view of the index should be maintained for scrolled search
     * @var integer
     */
    public $scroll;
    /**
     * Search operation type
     * @see YiiElasticSearch\Enum\SearchType
     * @var string
     */
    public $search_type;
    /**
     * Number of hits to return (default: 10)
     * @var integer
     */
    public $size = 10;
    /**
     * An key value pair array of <field>:<direction>
     * @var array
     */
    public $sort;
    /**
     * The URL-encoded request definition using the Query DSL (instead of using request body)
     * @var string
     */
    public $source;
    /**
     * True or false to return the _source field or not, or a list of
     * @var boolean|array
     */
    public $_source;
    /**
     * A list of fields to exclude from the returned _source field
     * @var array
     */
    public $_source_include = [];
    /**
     * A list of fields to extract and return from the _source field
     * @var array
     */
    public $_source_exclude = [];
    /**
     * The maximum number of documents to collect for each shard, upon reaching which the query execution will
     * terminate early
     * @var integer
     */
    public $terminate_after;

    /**
     * the internal data storage
     * @var array
     */
    protected $mData = array();

    /**
     * @param string|null $index the name of the index to search within
     * @param string|null $type the name of the document type
     * @param array $data the query data
     */
    public function __construct($index = null, $type = null, $data = array())
    {
        $this->mData = $data;
        $this->index = $index;
        $this->type = $type;

        parent::__construct($this->mData, ArrayObject::ARRAY_AS_PROPS);
    }

    /**
     * @return array an array representation of the query
     */
    public function toArray()
    {
        return $this->getArrayCopy();
    }

    /**
     * Get the named field value     *
     * @param string $name the name of the field to access
     * @return mixed the value
     *
     * @throws \Exception if no source field exists with the given name
     */
    public function __get($name)
    {
        if ($this->offsetExists($name)) {
            return $this->offsetGet($name);
        }

        throw new Exception(self::class." has no such property: {$name}");
    }

    /**
     * Sets the named field value
     * @param string $name the name of the field to set
     * @param mixed $value the value to set
     */
    public function __set($name, $value)
    {
        $this->offsetSet($name, $value);
    }

    /**
     * Determine whether or not the search has a field with the given name
     * @param string $name the field name
     *
     * @return bool
     */
    public function __isset($name)
    {
        return $this->offsetExists($name);
    }

    /**
     * Removes the named field.
     * @param string $name the name of the field to remove
     */
    public function __unset($name)
    {
        if ($this->offsetExists($name)) {
            $this->offsetUnset($name);
        } else {
            throw new Exception(self::class." has no such property: {$name}");
        }
    }
}
