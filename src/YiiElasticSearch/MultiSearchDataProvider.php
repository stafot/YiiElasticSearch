<?php
namespace YiiElasticSearch;

/**
 * A Data provider for searching into multiple indices
 */
class MultiSearchDataProvider extends DataProvider
{
    public $connID = 'elasticSearch';
    public $models = array();

    /**
     * Initialize the data provider
     * @param array $models A list with models to search with
     * @param array $config the data provider configuration
     */
    public function __construct(array $models, $config = array())
    {
        foreach ($models as $model) {
            if (is_string($model))
                $model = new $model;

            $this->models["{$model->elasticIndex}-{$model->elasticType}"] = $model;
        }

        foreach($config as $attribute => $value)
            $this->{$attribute} = $value;
    }

    /**
     * @param \YiiElasticSearch\Search|array $search a Search object or an array with search parameters
     */
    public function setSearch($search)
    {
        $indices    = array();
        $types      = array();

        array_walk($this->models, function($model) use ($indices, $types) {
            $indices[] = $model->elasticIndex;
            $types[]   = $model->elasticType;
        });

        $indices = array_unique($indices);
        $types   = array_unique($types);

        if (is_array($search)) {
            $search = new Search(
                implode(',', $indices),
                implode(',', $types),
                $search
            );
        }

        if(!$search->index) {
            $search->index = implode(',', $indices);
        }

        if(!$search->type) {
            $search->type =implode(',', $types);
        }

        $this->_search = $search;
    }

    /**
     * @return \YiiElasticSearch\Search
     */
    public function getSearch()
    {
        $indices    = array();
        $types      = array();

        array_walk($this->models, function($model) use ($indices, $types) {
            $indices[] = $model->elasticIndex;
            $types[]   = $model->elasticType;
        });

        $indices = array_unique($indices);
        $types   = array_unique($types);

        if ($this->_search === null) {
            $this->_search = new Search(
                implode(',', $indices),
                implode(',', $types),
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


            $this->resultSet = $this->getElasticConnection()->search($search);

            $this->fetchedData = array();
            foreach($this->resultSet->getResults() as $result) {
                $reflObject = new \ReflectionObject($this->models["{$result->getIndex()}-{$result->getType()}"]);
                $modelClass = $reflObject->getName();
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

    public function getElasticConnection()
    {
        return \Yii::app()->getComponent($this->connID);
    }
}