<?php

namespace YiiElasticSearch;

use CApplicationComponent as ApplicationComponent;
use CDataProvider;
use CDataProviderIterator;
use Elasticsearch\ClientBuilder;
use Yii;

/**
 * The elastic search connection is responsible for actually interacting with elastic search,
 * e.g. indexing documents, performing queries etc.
 *
 * It should be configured in the application config under the components array
 * <pre>
 *  'components' => array(
 *      'elasticSearch' => array(
 *          'class' => "YiiElasticSearch\\Connection",
 *          'hosts' => [
 *              'https://BASE64(user):BASE64(pass)@localhost:9200'
 *              --- OR ---
 *              [
 *                  'host'=>'localhost',
 *                  'port'=>'9200',
 *                  'scheme' => 'https'
 *                  'username'=>'user',
 *                  'passwrod'=>'pass',
 *                  ....
 *              ]
 *          ],
 *      )
 *  ),
 * </pre>
 *
 * @author Charles Pick <charles.pick@gmail.com>
 * @licence MIT
 * @package YiiElasticSearch
 */
class Connection extends ApplicationComponent
{
    /**
     * ElasticSearchclient host configuration.
     * @see Elastisearch\ClientBuilder
     * @var array
     */
    public $hosts = ["http://localhost:9200/"];

    /**
     * Number of retries on opening conection
     * @var integer
     */
    public $retries = 2;

    /**
     * @var boolean whether or not to profile elastic search requests
     */
    public $enableProfiling = false;

    /**
     * @var string an optional prefix for the index. Default is ''.
     */
    public $indexPrefix = '';

    /**
     * If true enalbes reposne verbosity. Debuga data should be retrived
     * via document respective property
     * @var boolean
     */
    public $debug = false;

    /**
     * Set the request timeout
     * @var integer
     */
    public $timeout = 10;

    /**
     * Set the connect timeout
     * @var integer
     */
    public $connectTimeout = 5;

    /**
     * Retry on these http response codes and retry until the number of max
     * retries are met.
     * @var array
     */
    public $exceptionsToIgnore = [404];

    /**
     * @var \Guzzle\Http\Client the guzzle client
     */
    protected $_client;

    /**
     * @var \Guzzle\Http\Client the async guzzle client
     */
    protected $_asyncClient;

    /**
     * @return \Guzzle\Http\Client
     */
    public function getAsyncClient()
    {
        if ($this->_asyncClient === null) {
            $this->_asyncClient = ClientBuilder::fromConfig([
                'hosts' => $this->hosts,
                'retries' => $this->retries,
                'handler' => ClientBuilder::defaultHandler()
            ], false);
        }
        return $this->_asyncClient;
    }

    /**
     * @return \Guzzle\Http\Client
     */
    public function getClient()
    {
        if ($this->_client === null) {
            $this->_client = ClientBuilder::fromConfig([
                'hosts' => $this->hosts,
                'retries' => $this->retries
            ], false);
        }
        return $this->_client;
    }

    /**
     * Add a document to the index
     * @param DocumentInterface $document the document to index
     * @param bool $async whether or not to perform an async request.
     *
     * @return \Guzzle\Http\Message\Response|mixed the response from elastic search
     */
    public function index(DocumentInterface $document, $async = false)
    {
        $client = $this->getAsyncClient();
        $params = $this->getRequestParams($document, $async);
        $response = $client->index($params);

        return $response;
    }

    /**
     * Bulk indexing
     * @param  CDataProvider   $documents A data providers with documents that
     *                                    implementing the DocumentInterface
     * @param  integer $batchSize The batch size. If there are more documents than the
     * @return array             [description]
     */
    public function bulkIndex(CDataProvider $documents, $batchSize = 500)
    {
        $client = $this->getClient();
        $params = ['body' => []];
        $responses = [];

        $iterator = new CDataProviderIterator($documents, $batchSize);
        $currentPage = 0;
        foreach ($iterator as $document) {
            if ($iterator->dataProvider->pagination->getCurrentPage(false) !== $currentPage) {
                // Send the batch
                $responses["batch {$currentPage}"] = $client->bulk($params);
                $params = ['body' => []];
            }

            $params['body'][] = [
                 'index' => [
                     '_type' => $document->type,
                     '_index' => $document->index,
                     '_parent' => $document->parent,
                     '_routing' => $document->routing,
                     '_id' => $document->id,
                     '_timestamp' => $document->timestamp,
                 ]
            ];
            $params['body'][] = $document->getSource();
            $currentPage = $iterator->dataProvider->pagination->getCurrentPage(false);
        }
        if (!empty($params['body'])) {
            $responses["batch {$currentPage}"] = $client->bulk($params);
        }
        return $responses;
    }

    /**
     * Remove a document from elastic searchfgfgghthtnmggg
     * @param DocumentInterface $document the document to remove
     * @param bool $async whether or not to perform an async request
     *
     * @return \Guzzle\Http\Message\Response|mixed the response from elastic search
     */
    public function delete(DocumentInterface $document, $async = false)
    {
        $client = $async ? $this->getAsyncClient() : $this->getClient();
        $request = $client->delete($document->getUrl());

        return $this->perform($request, $async);
    }

    /**
     * Perform an elastic search
     * @param Search $search the search parameters
     *
     * @return ResultSet the result set containing the response from elastic search
     */
    public function search(Search $search)
    {
        $query = json_encode($search->toArray());
        $url = array();
        if ($search->index)
            $url[] = $this->indexPrefix.$search->index;
        if ($search->type)
            $url[] = $search->type;

        $url[] = "_search";
        $url = implode("/",$url);

        $client = $this->getClient();
        $request = $client->post($url, null, $query);
        $response = $this->perform($request);
        return new ResultSet($search, $response);
    }


    /**
     * Perform a http request and return the response
     *
     * @param \Guzzle\Http\Message\RequestInterface $request the request to preform
     * @param bool $async whether or not to perform an async request
     *
     * @return \Guzzle\Http\Message\Response|mixed the response from elastic search
     * @throws \Exception
     */
    public function perform(\Guzzle\Http\Message\RequestInterface $request, $async = false)
    {
        try {
            $profileKey = null;
            if ($this->enableProfiling) {
                $profileKey = __METHOD__.'('.$request->getUrl().')';
                if ($request instanceof \Guzzle\Http\Message\EntityEnclosingRequest)
                    $profileKey .= " ".$request->getBody();
                Yii::beginProfile($profileKey);
            }
            $response = $async ? $request->send() : json_decode($request->send()->getBody(true), true);
            Yii::trace("Sent request to '{$request->getUrl()}'", 'application.elastic.connection');
            if ($this->enableProfiling)
                Yii::endProfile($profileKey);
            return $response;
        }
        catch (\Guzzle\Http\Exception\BadResponseException $e) {
            $body = $e->getResponse()->getBody(true);
            if(($msg = json_decode($body))!==null && isset($msg->error)) {
                throw new \CException(is_object($msg->error) ? $msg->error->reason : $msg->error);
            } else {
                throw new \CException($e);
            }
        }
        catch(\Guzzle\Http\Exception\ClientErrorResponseException $e) {
            throw new \CException($e->getResponse()->getBody(true));
        }
    }

    /**
     * @param string $url of resource to check e.g. /twitter/tweet
     * @return bool whether there are documents for this type
     */
    public function typeEmpty($url)
    {
        $url = '/'.trim($url,'/').'/_count';
        try {
            $response = $this->getClient()->get($url)->send()->json();
            return !isset($response['count']) || !$response['count'];
        }
        catch (\Guzzle\Http\Exception\BadResponseException $e) { }
        catch(\Guzzle\Http\Exception\ClientErrorResponseException $e) { }

        return false;
    }

    /**
     * @param string $url the resource URL to check e.g. /twitter or /twitter/tweet
     * @return bool whether a mapping exists for the given resource
     */
    public function mappingExists($url)
    {
        $url = '/'.trim($url,'/').'/_mapping';
        try {
            $response = $this->getClient()->get($url)->send();
            return true;
        }
        catch (\Guzzle\Http\Exception\BadResponseException $e) { }
        catch(\Guzzle\Http\Exception\ClientErrorResponseException $e) { }

        return false;
    }

    /**
     * Escapes the following terms:
     * + - && || ! ( ) { } [ ] ^ " ~ * ? : \
     *
     * @param $term
     * @return string
     * @link http://lucene.apache.org/core/3_4_0/queryparsersyntax.html#Escaping%20Special%20Characters
     */
    public function escape($term)
    {
        $result = $term;
        // \ escaping has to be first, otherwise escaped later once again
        $chars = array('\\', '+', '/', '-', '&&', '||', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':');

        foreach ($chars as $char) {
            $result = str_replace($char, '\\' . $char, $result);
        }
        return trim($result);
    }

    public function getRequestParams(DocumentInterface $document)
    {
        $params = [
            'type' => $document->type,
            'index' => $document->index,
            'parent' => $document->parent,
            'routing' => $document->routing,
            'id' => $document->id,
            'body' => $document->source,
            'timestamp' => $document->timestamp,
            'client' => $this->getPerRequestConfig($document, $async)
        ];


        return $params;
    }

    public function getPerRequestConfig(DocumentInterface $document, $async = false)
    {
        $params = [
            'igonore' => $document->exceptionsToIgnore,
            'verbose' => $document->connection->debug ? true : false,
            'timeout' => $document->timeout,
            'connect_timeout' => $document->connectTimeout
        ];

        if ($async) {
            $params['future'] = 'lazy';
        }

        return $params;
    }
}
