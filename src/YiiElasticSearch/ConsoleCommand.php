<?php
namespace YiiElasticSearch;

use \Yii as Yii;
use \CConsoleCommand as CConsoleCommand;
use \CActiveRecord as CActiveRecord;

/**
 * This is the maintenance command for the elasticSearch component.
 *
 * @author Michael Härtl <haertl.mike@gmail.com>
 * @licence MIT
 * @package YiiElasticSearch
 */
class ConsoleCommand extends CConsoleCommand
{
    /**
     * @var string name of default action.
     */
    public $defaultAction = 'help';

    /**
     * @var string name of model
     */
    public $model;

    /**
     * @var string name of index
     */
    public $index;

    /**
     * @var string name of type
     */
    public $type;

    /**
     * @var bool whether to supress any output from this command
     */
    public $quiet = false;

    /**
     * @var bool whether to be more verbose
     */
    public $verbose = false;

    /**
     * @var bool whether to only perform the command if target does not exist. Default is false.
     */
    public $skipExisting = false;
    /**
     * @var string addition options for a creating index
     */
    public $options = '{}';

    /**
     * @return string help for this command
     */
    public function getHelp()
    {
        return <<<EOD
USAGE
  yiic elastic [action] [parameter]

DESCRIPTION
  This is the maintenance command for the elasticsearch component. It
  provides utilities to manage or list entries in the elasticsearch index.

ACTIONS

  * index --model=<model> [--skipExisting] [--deleteType]

    Add all models <model> to the index. This will replace any previous
    entries for this model in the index. Index and type will be auto-detected
    from the model class unless --index or --type is set explicitely.
    If --skipExisting is used, no action is performed if there are already
    documents indexed under this type.
    If --deleteType is used, the the model's type will be deleted before indexing.
    After deleting type, indexing works only if automatic index creation is enabled
    in elasticsearch, mapping is also lost.


  * map --model=<model> --map=<filename> [--skipExisting]
    map --index=<index> --map=<filename> [--skipExisting]

    Create a mapping in the index specified with the <index> or implicitly
    through the <model> parameter. The mapping must be available from a JSON
    file in <filename> where the JSON must have this form:

        {
            "tweet" : {
                "properties": {
                    "name" : {"type" : "string"},
                    ...
            },
            ...
        }

    If --skipExisting is used, no action is performed if there's are already
    a mapping for this index.


  * list [--limit=10] [--offset=0]
    list [--model=<name>] [--limit=10] [--offset=0]
    list [--index=<name>] [--type=<type>] [--limit=10] [--offset=0]

    List all entries in elasticsearch. If a model or an index (optionally with
    a type) is specified only entries matching index and type of the model will be listed.


  * delete --model=<model> [--id=<id>]
    delete --index=<index>

    Delete a document, type or the whole index.
    Document is deleted if <model> and <id> is specified.
    A type is deleted if only <model> is specified.
    If <index> is specified the whole index is deleted.


  * help

    Show this help

EOD;
    }

    /**
     * Index the given model in elasticsearch
     */
    public function actionIndex($deleteType=false)
    {
        $n = 0;

        $model  = $this->getModel();
        $table  = $model->tableName();
        $provider = new \CActiveDataProvider($model);
        $count = $provider->totalItemCount;

        $step   = $count > 5 ? floor($count/5) : 1;
        $index  = Yii::app()->elasticSearch->indexPrefix.$model->elasticIndex;
        $type   = $model->elasticType;

        /**
         * If $deleteType is true we delete the type before indexing.
         * After deleting type, indexing works only if automatic index creation is enabled, also mapping is lost.
         * Information about auto index creation: http://www.elasticsearch.org/guide/reference/api/index_/1
         */
        if ($deleteType) {
            $this->message("Type '$index/$type' deleted.");
            $this->performRequest(Yii::app()->elasticSearch->client->delete("$index/$type"));
        }

        if($this->skipExisting && !Yii::app()->elasticSearch->typeEmpty("$index/$type")) {
            $this->message("'$index/$type' is not empty. Skipping index command.");
            return;
        }

        $this->message("Adding $count '{$this->model}' records from table '$table' to index '$index'\n 0% ", false);

        $iterator = new \CDataProviderIterator($provider);
        foreach($iterator as $record) {
            $record->indexElasticDocument();
            $n++;

            if(($n % $step)===0) {
                $percent = 20*$n / $step;

                if($percent < 100) {
                    $this->message((20*$n/$step).'% ',false);
                }
            }
        }

        $this->message('100%');
    }

    /**
     * @param string $map the path to the JSON map file
     * @param bool $noDelete whether to supress index deletion
     */
    public function actionMap($map)
    {
        $index      = $this->getIndex(true, true);
        $file       = file_get_contents($map);
        $mapping    = json_decode($file);
        $elastic    = Yii::app()->elasticSearch;

        if($elastic->mappingExists($index)) {
            if($this->skipExisting) {
                $this->message("Mapping for '$index' exists. Skipping map command.");
                return;
            } else {
                $this->message("Deleting '$index' ... ",false);
                $this->performRequest($elastic->client->delete($index));
                $this->message("done");
            }
        }

        if($mapping===null) {
            $this->usageError("Invalid JSON in $map");
        }

        $body = json_encode(\CMap::mergeArray(array(
            'mappings' => $mapping,
        ), json_decode($this->options, true)));

        $this->performRequest($elastic->client->put($index, array("Content-type" => "application/json"))->setBody($body));

        $this->message("Created mappings for '$index' from file in '$map'");
    }


    /**
     * List documents in elasticsearch
     *
     * @param int $limit how many documents to show. Default is 10.
     * @param int $offset at which document to start. Default is 0.
     */
    public function actionList($limit=10, $offset=0)
    {
        $search         = new Search;
        $search->size   = $limit;
        $search->from   = $offset;
        if(($model = $this->getModel(false))!==null) {
            $search->index  = $model->elasticIndex; // use unprefixed index
            $search->type   = $model->elasticType;
            $_i = Yii::app()->elasticSearch->indexPrefix.$search->index;
            $this->message("Index: {$_i} Type: {$search->type}"); // prefixed index will be used for search by connection
        } else {
            if(($index = $this->getIndex(false))!==null) { // use unprefixed index
                $search->index = $index;
                $this->message("Index: {$this->getIndex(false, true)}"); // prefixed index will be used for search by connection
            }
            if(($type = $this->getType(false))!==null) {
                $search->type = $type;
                $this->message("Type: {$search->type}");
            }
        }

        $search->query = array(
            'match_all' => array(),
        );

        try {
            $result = Yii::app()->elasticSearch->search($search);
        } catch (\CException $e) {
           $this->message($e->getMessage());
            Yii::app()->end(1);
        }

        $this->message("Showing {$result->count} of {$result->total} found documents");
        $this->message('-------------------------------------------------------');
        foreach($result->results as $document) {
            $this->renderDocument($document);
        }
    }

    /**
     * Delete a document, type or a complete index from elasticsearch
     *
     * @param int|null $id of the record to delete. Optional.
     */
    public function actionDelete($id=null)
    {
        $urlParts = array();

        if ($this->model) { // Delete a document or a type
            $model = $this->getModel(true);
            $index = $urlParts[] = Yii::app()->elasticSearch->indexPrefix.$model->elasticIndex;
            $urlParts[] = $model->elasticType;

            if ($id) {
                $urlParts[] = $id;
                $this->message("Deleting #{$id} document from '{$index}/{$model->elasticType}': ", false);
            } else {
                $this->message("Deleting '{$model->elasticType}' type from index '{$index}': ", false);
            }

        } else { // Delete a whole index
            $index = $this->getIndex(true, true);
            $urlParts[] = $index;
            $this->message("Deleting index '{$index}': ", false);
        }

        $this->performRequest(Yii::app()->elasticSearch->client->delete(implode('/', $urlParts)));

        $this->message("done");
    }

    /**
     * Show help
     */
    public function actionHelp()
    {
        echo $this->getHelp();
    }

    /**
     * Output a message
     *
     * @param string $text message text
     * @param bool $newline whether to append a newline. Default is true.
     */
    protected function message($text, $newline=true)
    {
        if(!$this->quiet) {
            echo $text . ($newline ? "\n" : '');
        }
    }

    /**
     * Output a document
     *
     * @param Document $document the document to render
     */
    protected function renderDocument($document)
    {
        $this->message("Index   : {$document->getIndex(true,true)}");
        $this->message("Type    : {$document->getType()}");
        $this->message("ID      : {$document->getId()}");
        if($this->verbose) {
            $this->message('.......................................................');
            foreach($document as $key=>$value) {
                $this->message(sprintf(' %20s : %20s',$key,$this->parseValue($value)));
            }
        }

        $this->message('-------------------------------------------------------');
    }

    /**
     * @param mixed $value any document value
     * @return string the parsed value ready for output
     */
    protected function parseValue($value)
    {
        if(is_array($value)) {
            $values = '[';
            if(isset($value[0])) {
                $values .= $this->parseValue($value[0]);
            }
            if(isset($value[1])) {
                $values .= ',...]';
            } else {
                $values .= ']';
            }
            return $values;
        } else {
            return $value;
        }
    }

    /**
     * @param Guzzle\EntityEnclosingRequestInterface $request
     */
    protected function performRequest($request)
    {
        try {
            $request->send();
        } catch (\Guzzle\Http\Exception\BadResponseException $e) {
            $body = $e->getResponse()->getBody(true);
            if(($msg = json_decode($body))!==null && isset($msg->error)) {
                $this->message(is_object($msg->error) ? $msg->error->reason : $msg->error);
            } else {
                $this->message($e->getMessage());
            }
            Yii::app()->end(1);
        }
        catch(\Guzzle\Http\Exception\ClientErrorResponseException $e) {
            $this->message($e->getResponse()->getBody(true));
            Yii::app()->end(1);
        }
    }

    /**
     * @param bool $required whether a model is required
     * @return CActiveRecord|null the model instance
     */
    protected function getModel($required=true)
    {
        if(!$this->model) {
            if($required) {
                $this->usageError("Model must be supplied with --model.");
            } else {
                return null;
            }
        }

        return CActiveRecord::model($this->model);
    }

    /**
     * @param bool $required whether a index is required
     * @return string|null the prefix and index name as set with --index or implicitly through --model
     */
    protected function getIndex($required=true, $withPrefix=false)
    {
        if(!$this->model && !$this->index) {
            if($required) {
                $this->usageError("Either --model or --index must be supplied.");
            } else {
                return null;
            }
        }
        $prefix = ($withPrefix) ? Yii::app()->elasticSearch->indexPrefix : '';

        return $this->index ? $prefix.$this->index : $prefix.$this->getModel()->elasticIndex;
    }

    /**
     * @param whether a type is required
     * @return string|null the type name as set with --type or implicitly through --model
     */
    protected function getType($required=true)
    {
        if(!$this->model && !$this->type) {
            if($required) {
                $this->usageError("Either --model or --type must be supplied.");
            } else {
                return null;
            }
        }

        return $this->type ? $this->type : $this->getModel()->elasticType;
    }
}
