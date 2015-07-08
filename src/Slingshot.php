<?php
namespace vpg\slingshot;
require_once 'vendor/autoload.php';

use Monolog;

/**
 * ES Migration tool based on Scann/Scroll + bulk
 *
 * @todo
 *  - Add log
 *  - Support differents hosts (source and target) to allow index copy from one srv to another
 *  - Add custom- Support differents hosts (source and target) to allow index copy from one srv to another
 *
 * @namespace vpg\slingshot
 */
class Slingshot
{
    private $ESClientSource;
    private $ESClientTarget;
    private $migrationHash;

    private $convertDocCallBack;

    /**
     * Instanciate the ElasticSearch Migration Service
     *
     * @param array $esCnfHash     Elastic Search connection config
     *              e.g. : [
     *                  'from' => '192.168.100.100:9200',
     *                  'to'   => '192.168.100.100:9200'
     *               ]
     * @param array $migrationHash index migration informations
     *              e.g. : [
     *                  'from'    => ['index' => 'bar',     'type' => 'foo'],
     *                  'to'      => ['index' => 'new_bar', 'type' => 'foo'],
     *                  'bulk'    => ['action' => 'index', 'id' => '_id'],
     *                  'aliases' => ['read' => 'barRead', 'write' => 'barWrite'],
     *                  'mapping' => [...]
     *              ]
     *
     * @return void
     */
    public function __construct($esCnfHash, $migrationHash) {
        if ($this->isMigrationConfValid($migrationHash)) {
            throw new \Exception('Wrong migrationHash paramater');
        }
        $this->migrationHash = $migrationHash;
        $this->ESClientSource = new \Elasticsearch\Client(['hosts' => [$esCnfHash['from']]);
        $this->ESClientTarget = &$this->ESClientSource;
        if ($esCnfHash['from'] != $esCnfHash['to']) {
            $this->ESClientTarget = new \Elasticsearch\Client(['hosts' => [$esCnfHash['to']]);
        }
    }

    /**
     * Migrates the documents matching the given $searchQueryHash query to the new index.
     * Apply for each doc the callback func $convertDocCallBack which returns the doc to index.
     *
     * @param array    $searchQueryHash    Elastic Search query, used to select document to migrate
     *                 default null, match all docs
     *                 e.g. : [
     *                 ]
     * @param function $convertDocCallBack Callback function to convert document from the old index to the new one.
     *                 prototype : array convertDocCallBack(array oldIndexDocument)
     *
     * @return void
     */
    public function migrate(array $searchQueryHash = [], $convertDocCallBack = null) 
    {
        $this->searchQueryHash = $searchQueryHash;
        if (!is_callable($convertDocCallBack)) {
            throw new \Exception('Wrong callback function');
        }
        $this->convertDocCallBack = $convertDocCallBack;
        $this->processDocumentsMigration();
    }

    private function processDocumentsMigration()
    {
        $searchHashBase = [
            "search_type" => "scan",
            "scroll" => "30s",
            "size" => 1000,
        ];
        $searchHash = array_merge($searchHashBase, $this->migrationHash['from']);
        if ($this->searchQueryHash) {
            $searchHash['body']['query'] = $this->searchQueryHash;
        }
        $docs = $this->ESClientSource->search($searchHash);
        $scroll_id = $docs['_scroll_id'];

        $iDoc = 0;
        $bulkHash = $this->migrationHash['to'];
        while (true) {
            $response = $this->ESClientSource->scroll(
                array(
                    "scroll_id" => $scroll_id,
                    "scroll" => "30s"
                )
            );
            $docNb = count($response['hits']['hits']);
            if ($docNb > 0) {
                foreach ($response['hits']['hits'] as $hitHash) {
                    $iDoc++;
                    $docHash = call_user_func($this->convertDocCallBack, $hitHash['_source']);
                    $bulkHash['body'][] = [
                        $this->migrationHash['bulk']['action'] => [ '_id' => $docHash['_id']]
                    ];
                    $bulkHash['body'][] = $docHash;
                    if (!($iDoc % 10000)) {
                        echo "Bulk index batch of 10000 docs\n";
                        $r = $this->ESClientTarget->bulk($bulkHash);
                        $bulkHash = $this->migrationHash['to'];
                    }
                }
                // Get new scroll_id
                $scroll_id = $response['_scroll_id'];
            } else {
                break;
            }
        }
    }

    private function isMigrationConfValid($migrationHash)
    {
        if ( empty($migrationHash['from']['index'])
            || empty($migrationHash['from']['type'])
            || empty($migrationHash['to']['index'])
            || empty($migrationHash['to']['type'])
        ) {
            return false;
        }
        return true;
    }

}
