<?php

namespace YiiElasticSearch;

/**
 * Represents an elastic search result document
 *
 * @author Charles Pick <charles.pick@gmail.com>
 * @licence MIT
 * @package YiiElasticSearch
 */
class SearchResult extends Document
{
    /**
     * @var ResultSet the result set this result is part of
     */
    protected $_resultSet;

    /**
     * @var float the result score
     */
    protected $_score;

    /**
     * An array with the selected field in the result
     * useful with dynamic fields
     * @var array
     */
    protected $_fields;

    /**
     * Initialize the search result
     * @param ResultSet $resultSet the result set this is a part of
     * @param array $result the result data
     */
    public function __construct(ResultSet $resultSet, array $result)
    {
        $this->_resultSet = $resultSet;
        $this->_index = $result['_index'];
        $this->_type = $result['_type'];
        $this->_id = $result['_id'];
        $this->_score = $result['_score'];
        $this->_source = $result['_source'];
        $this->_fields = isset($result['fields']) ? $result['fields'] : [];
    }

    /**
     * @return float the result score
     */
    public function getScore()
    {
        return $this->_score;
    }

    /**
     * @return \YiiElasticSearch\ResultSet the result set this result is part of
     */
    public function getResultSet()
    {
        return $this->_resultSet;
    }

    /**
     * Return the value of the field from the field list
     * @param  string $field
     * @return mixed
     */
    public function getField($field)
    {
        return $this->_fields[$field];
    }
}
