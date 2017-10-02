<?php

namespace YiiElasticSearch;

use CDataProvider;

/**
 * Data provider that can retrieve results from elastic search.
 *
 * <pre>
 *  $dataProvider = new YiiElasticSearch\DataProvider(MySearchableModel::model());
 *  CVarDumper::dump($dataProvider->getData());
 * </pre>
 *
 * @package YiiElasticSearch
 */
class DataProvider extends CDataProvider
{
    /**
     * @var CActiveRecord a model implementing the searchable interface
     */
    public $model;

    /**
     * @var string|null optional name of model attribute to use a attribute key instead of the primary key
     */
    public $keyAttribute;

    /**
     * @var Search the search parameters
     */
    protected $_search;

    /**
     * @var ResultSet the search result set
     */
    public $resultSet;

    /**
     * @var mixed the fetched data
     */
    protected $fetchedData;

    /**
     * Initialize the data provider
     * @param CActiveRecord $model the search model
     * @param array $config the data provider configuration
     */
    public function __construct($model, $config = array())
    {
        if (is_string($model)) {
            $model = new $model;
        }

        $this->model = $model;

        foreach ($config as $attribute => $value) {
            $this->{$attribute} = $value;
        }
    }

    /**
     * @param \YiiElasticSearch\Search|array $search a Search object or an array with search parameters
     */
    public function setSearch($search)
    {
        if (is_array($search)) {
            $search = new Search(
                $this->model->elasticIndex,
                $this->model->elasticType,
                $search
            );
        }

        if (!$search->index) {
            $search->index = $this->model->elasticIndex;
        }

        if(!$search->type) {
            $search->type = $this->model->elasticType;
        }

        $this->_search = $search;
    }

    /**
     * @return \YiiElasticSearch\Search
     */
    public function getSearch()
    {
        if ($this->_search === null) {
            $this->_search = new Search(
                $this->model->elasticIndex,
                $this->model->elasticType,
                array(
                    'query' => array(
                        'match_all' => array()
                    )
                )
            );
        }
        return $this->_search;
    }

    /**
     * @return array the facets
     */
    public function getFacets()
    {
        if($this->resultSet===null) {
            $this->fetchData();
        }

        return $this->resultSet->getFacets();
    }

    /**
     * @return array list of data items
     */
    protected function fetchData()
    {
        if($this->fetchedData===null) {
            $search = $this->_search;
            if (($pagination = $this->getPagination()) !== false) {
                $pagination->validateCurrentPage = false;
                $search['from'] = $pagination->getOffset();
                $search['size'] = $pagination->pageSize;
            }


            $this->resultSet = $this->model->getElasticConnection()->search($search);

            $this->fetchedData = array();
            $modelClass = get_class($this->model);
            foreach($this->resultSet->getResults() as $result) {
                $model = new $modelClass;
                $model->setIsNewRecord(false);
                $model->parseElasticDocument($result);
                $this->fetchedData[] = $model;
            }

            if($pagination!==false)
            {
                $pagination->setItemCount($this->getTotalItemCount());
            }
        }
        return $this->fetchedData;
    }

    /**
     * @return array list of data item keys.
     */
    protected function fetchKeys()
    {
        $keys=array();
        foreach($this->getData() as $i=>$data)
        {
            $key=$this->keyAttribute===null ? $data->getPrimaryKey() : $data->{$this->keyAttribute};
            $keys[$i]=is_array($key) ? implode(',',$key) : $key;
        }
        return $keys;
    }

    /**
     * @return integer the total number of data items.
     */
    protected function calculateTotalItemCount()
    {
        if($this->resultSet===null) {
            $this->fetchData();
        }

        return $this->resultSet->getTotal();
    }
}
