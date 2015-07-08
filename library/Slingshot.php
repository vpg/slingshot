<?php
namespace vpg\slingshot;

/**
 * ES Migration tool based on Scann/Scroll + bulk
 *
 * @todo
 *  - Add log using monolog
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
     * @param array $hostsConfHash Elastic Search connection config
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
    public function __construct($hostsConfHash, $migrationHash)
    {
        if (!$this->isMigrationConfValid($migrationHash)) {
            throw new \Exception('Wrong migrationHash paramater');
        }
        if (!$this->isHostsConfValid($hostsConfHash)) {
            throw new \Exception('Wrong hosts conf paramater');
        }
        $this->migrationHash = $migrationHash;
        $this->ESClientSource = new \Elasticsearch\Client(['hosts' => [$hostsConfHash['from']]]);
        $this->ESClientTarget = &$this->ESClientSource;
        if ( !empty($hostsConfHash['to']) &&  $hostsConfHash['from'] != $hostsConfHash['to']) {
            $this->ESClientTarget = new \Elasticsearch\Client(['hosts' => [$hostsConfHash['to']]]);
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

    /**
     * Processes the migration using scan & scroll search
     * Applies the given callback func on each document
     * Saves document by batch using bulk API
     *
     * @todo factorize
     *
     * @return void
     */
    private function processDocumentsMigration()
    {
        $scanQueryHash = $this->buildScanQueryHash();
        $searchResultHash = $this->ESClientSource->search($scanQueryHash);
        $totalDocNb = $searchResultHash['hits']['total'];
        $scrollId = $searchResultHash['_scroll_id'];

        $iDoc = 0;
        $bulkHash = $this->migrationHash['to'];
        $bulkAction = $this->migrationHash['bulk']['action'];
        $bulkBatchSize = $this->migrationHash['bulk']['batchSize'];
        while (true) {
            $response = $this->ESClientSource->scroll(
                array(
                    "scroll_id" => $scrollId,
                    "scroll" => "30s"
                )
            );
            $docNb = count($response['hits']['hits']);
            // If there is nothing to process anymore
            if ($docNb <= 0) {
                break;
            }
            foreach ($response['hits']['hits'] as $hitHash) {
                $docHash = call_user_func($this->convertDocCallBack, $hitHash['_source']);
                $bulkHash['body'][] = [
                    $bulkAction => [ '_id' => $docHash['_id']]
                    ];
                $bulkHash['body'][] = $docHash;
                $iDoc++;
                // Bulk index the batch size doc or the remaining doc
                if ( !($iDoc % $bulkBatchSize) || !($totalDocNb-$iDoc)) {
                    echo "\nBulk " . (count($bulkHash['body'])/2);
                    $r = $this->ESClientTarget->bulk($bulkHash);
                    $bulkHash = $this->migrationHash['to'];
                }
            }
            // Get new scroll id
            $scrollId = $response['_scroll_id'];
        }
    }


    /**
     * Builds scann search hash using the given migrationHash and searchQueryHash
     * Allows to averride scroll time and scroll size params
     *
     * @return array elastic search query
     */
    private function buildScanQueryHash()
    {
        $searchBaseHash = [
            "search_type" => "scan",
            ];
        $searchDefaultHash = [
            "scroll" => "30s",
            "size" => 1000,
            ];
        $searchHash = $searchBaseHash + $this->migrationHash['from'];
        $searchHash = array_merge($searchDefaultHash, $searchHash);
        if ($this->searchQueryHash) {
            $searchHash['body']['query'] = $this->searchQueryHash;
        }
        return $searchHash;
    }

    /**
     * Validates the given migration conf
     *
     * @param array $migrationHash index migration informations
     *
     * @return bool true if the migration conf is usable
     */
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

    /**
     * Validates the given elastic hosts conf
     *
     * @todo improve testing
     *
     * @param array $esConfHash index migration informations
     *
     * @return bool true if the migration conf is usable
     */
    private function isHostsConfValid($hostsConfHash)
    {
        if ( empty($hostsConfHash['from'])) {
            return false;
        }
        return true;
    }
}
