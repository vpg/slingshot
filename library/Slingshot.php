<?php
namespace vpg\slingshot;

use Monolog\Logger;
use Monolog\Handler\SyslogHandler;
use Monolog\Formatter\LineFormatter;
use Monolog\Processor\PsrLogMessageProcessor;

/**
 * ES Migration tool based on Scann/Scroll + bulk
 *
 * @todo
 *  - draft log w/ syslog
 *
 * @namespace vpg\slingshot
 */
class Slingshot
{
    private $ESClientSource;
    private $ESClientTarget;
    private $migrationHash;
    private $convertDocCallBack;
    private $bulkHash;

    private $logger;
    private $logLineFormat = "%channel% - %level_name%: %message%";

    const VERBOSITY_DEBUG = Logger::DEBUG;
    const VERBOSITY_INFO = Logger::INFO;
    const VERBOSITY_ERROR = Logger::ERROR;

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
     * @param integer log level Slingshot::VERBOSITY_INFO | VERBOSITY_DEBUG | VERBOSITY_ERROR
     * @return void
     */
    public function __construct($hostsConfHash, $migrationHash, $verbosity = self::VERBOSITY_INFO)
    {
        // Init logger
        $this->logger = new Logger('slingshot');
        $syslogH = new SyslogHandler(null, 'local6', $verbosity);
        $formatter = new LineFormatter($this->logLineFormat);
        $syslogH->setFormatter($formatter);
        $this->logger->pushHandler($syslogH);
        $this->logger->pushProcessor(new PsrLogMessageProcessor);

        if (!$this->isMigrationConfValid($migrationHash)) {
            throw new \Exception('Wrong migrationHash paramater');
        }
        if (!$this->isHostsConfValid($hostsConfHash)) {
            throw new \Exception('Wrong hosts conf paramater');
        }
        $this->migrationHash = $migrationHash;
        $this->docsProcessed = 0;
        $this->ESClientSource = new \Elasticsearch\Client(['hosts' => [$hostsConfHash['from']]]);
        if ( !empty($hostsConfHash['to']) &&  $hostsConfHash['from'] != $hostsConfHash['to']) {
            $this->ESClientTarget = new \Elasticsearch\Client(['hosts' => [$hostsConfHash['to']]]);
        }
        else {
            $this->ESClientTarget = &$this->ESClientSource;
        }
        $this->bulkHash = $this->migrationHash['to'];
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
        $startsAt = microtime(true);
        $this->logger->addInfo("Migration starts at {now}", ['now' => date('Y-m-d H:i:s')]);
        $this->searchQueryHash = (empty($searchQueryHash) || isset($searchQueryHash['query'])) ? $searchQueryHash : ['query' => $searchQueryHash];
        if (!is_callable($convertDocCallBack)) {
            throw new \Exception('Wrong callback function');
        }
        if (!$this->ESClientTarget->indices()->exists(['index' => $this->migrationHash['to']['index']])) {
            throw new \Exception('Target index does not exists! ' . $this->migrationHash['to']['index']);
        }
        $this->convertDocCallBack = $convertDocCallBack;
        $this->processMappingChanges();
        if ($this->migrationHash['withScroll']) {
            $this->logger->addInfo("withScroll");
            $this->processDocumentsMigrationWithScrolling();
        } else {
            $this->processDocumentsMigration();
        }
        // Stats
        $endsAt = microtime(true);
        $execTime = round(($endsAt - $startsAt)/60,3);
        $this->logger->addInfo("Migration ends at {now} in {time}m using {mem}Mo max for batch : {batchNb}",
            [
                'now' => date('Y-m-d H:i:s'),
                'time' => $execTime,
                'mem' => (memory_get_peak_usage(true)/1048576),
                'batchNb' => isset($this->migrationHash['documentsBatch']['batchNb']) ? $this->migrationHash['documentsBatch']['batchNb'] : ''
            ]
        );
    }

    /**
     * Processes the mapping changes if needed
     *
     * @return void
     */
    private function processMappingChanges()
    {
        if (empty($this->migrationHash['mappings']) || !is_array($this->migrationHash['mappings'])) {
            $this->logger->addInfo("No Mapping changes required for {index}/{type}", $this->migrationHash['to']);
            return false;
        }
        // fetch current mapping for FROM index/type
        $currentMappingHash = $this->ESClientSource->indices()->getMapping($this->migrationHash['from']);
        if (empty($currentMappingHash
                    [$this->migrationHash['from']['index']]
                    ['mappings']
                    [$this->migrationHash['from']['type']])
           ) {
            // if empty initialize mapping
            $newMappingPropertiesHash = $this->migrationHash['mappings'];
        } else {
            // replace current mapping properties with new mapping properties
            $newMappingPropertiesHash = array_replace_recursive(
                            $currentMappingHash[$this->migrationHash['from']['index']]['mappings'][$this->migrationHash['from']['type']],
                            $this->migrationHash['mappings']);
        }

        $newMappingHash = array(
            'index' => $this->migrationHash['to']['index'],
            'type' =>  $this->migrationHash['to']['type'],
            'body' => array($this->migrationHash['to']['type'] => $newMappingPropertiesHash)
        );

        $responseHash = $this->ESClientTarget->indices()->putMapping($newMappingHash);
        if (!empty($responseHash['acknowledged'])) {
            $this->logger->addInfo("Mapping succesfully changed for {index}/{type}", $this->migrationHash['to']);
        };
    }

    /**
    * Return all features for the source index ( mappings, aliases, settings, warmers ...)
    *
    * @return array
    */
    public function getSourceIndexFeatures()
    {
        $sourceIndexFeatures = $this->ESClientSource->indices()->get(
            [
                'index' => $this->migrationHash['from']['index']
            ]
        );

        return $sourceIndexFeatures[$this->migrationHash['from']['index']];
    }

    /**
     * Return all features for the source index ( mappings, aliases, settings, warmers ...)
     *
     */
    public function createTargetIndex($indexBody)
    {
        if ($this->ESClientTarget->indices()->exists(['index' => $this->migrationHash['to']['index']])) {
            $this->logger->addInfo(
                "Target index {targetIndex} exists already!",
                [
                    'targetIndex' => $this->migrationHash['to']['index']
                ]
            );
            return false;
        }
        $this->ESClientTarget->indices()->create(['index' => $this->migrationHash['to']['index'], 'body' => $indexBody]);
        $this->logger->addInfo(
            "Created target index: {targetIndex} !",
            [
                'targetIndex' => $this->migrationHash['to']['index']
            ]
        );
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
    private function processDocumentsMigrationWithScrolling()
    {
        $scanQueryHash = $this->buildScanQueryHash();
        $searchResultHash = $this->ESClientSource->search($scanQueryHash);
        $this->totalDocNb = $searchResultHash['hits']['total'];
        $this->logger->addInfo("Documents to migrate : {nb}", ['nb' => $this->totalDocNb]);
        $scrollId = $searchResultHash['_scroll_id'];
        while (true) {
            $this->logger->addDebug("Fetching new scroll ...");
            $scrollStartsAt = microtime(true);
            $response = $this->ESClientSource->scroll(
                array(
                    "scroll_id" => $scrollId,
                    "scroll" => "30s"
                )
            );
            $docNb = count($response['hits']['hits']);
            $scrollEndsAt = microtime(true);
            $scrollExecTime = round(($scrollEndsAt - $scrollStartsAt),3);
            $this->logger->addInfo("{nb} doc(s) scroll fetched in {time}s", ['nb' => $docNb, 'time' => $scrollExecTime]);
            // If there is nothing to process anymore
            if ($docNb <= 0) {
                break;
            }
            $this->processDocumentMigrationBatch($response);
            // Get new scroll id
            $scrollId = $response['_scroll_id'];
        }
    }

    /**
     * Processes the migration iterating through a batch of documents
     * Applies the given callback func on each document and saves them by bulk
     * Saves document by batch using bulk API
     *
     * @todo factorize
     *
     * @return void
     */
    private function processDocumentsMigration()
    {
        $searchHash = $this->migrationHash['from'];
        if ($this->searchQueryHash) {
            $searchHash['body'] = $this->searchQueryHash;
        }

        $searchHash['body']['from'] = $this->migrationHash['documentsBatch']['batchNb'] * $this->migrationHash['documentsBatch']['batchSize'];
        $searchHash['body']['size'] = $this->migrationHash['documentsBatch']['batchSize'];
        $this->logger->addDebug("Exec {query}", [ "query" => json_encode($searchHash)]);
        $response = $this->ESClientSource->search($searchHash);
        $docNb =  count($response['hits']['hits']);
        $this->totalDocNb = $docNb;
        $this->processDocumentMigrationBatch($response);
    }


    private function processDocumentMigrationBatch($response)
    {
        $this->logger->addInfo( "Start migration of {docNb} doc at {time}", [ "docNb" => count($response['hits']['hits']) , "time" => microtime(true) ]);
        $bulkAction = $this->migrationHash['bulk']['action'];
        $bulkBatchSize = $this->migrationHash['bulk']['batchSize'];
        $batchTimings = array();
        foreach ($response['hits']['hits'] as $hitHash) {

            $docHash = call_user_func($this->convertDocCallBack, $hitHash['_source'], $hitHash['_id']);

            // No doc, no processing
            if (is_null($docHash)) continue;

             //handle doc splitting
            if ($this->shouldSplittDoc($docHash)) {
                $this->logger->addDebug(
                    "Split Mode : doc {id} split in {splitNb}",
                    [ "id" => $hitHash['_id'], "splitNb" => count($docHash) ]
                );
                foreach ($docHash as $subDoc) {
                    $this->bulkHash['body'][] = [
                        $bulkAction => [ '_id' => isset($subDoc['_id']) ? $subDoc['_id'] : $subDoc['id']]
                    ];
                    $this->bulkHash['body'][] = $subDoc;
                }
            }
            else {
                $this->bulkHash['body'][] = [
                    $bulkAction => [ '_id' => $hitHash['_id']]
                ];
                $this->bulkHash['body'][] = $docHash;
            }
            $this->docsProcessed++;
            // Bulk index the batch size doc or the remaining doc
            if ( !($this->docsProcessed % $bulkBatchSize) || ($this->totalDocNb - $this->docsProcessed) <= 0) {
                $this->logger->addInfo("Send bulk at {time}", [ "time" => microtime(true) ]);
                $r = $this->ESClientTarget->bulk($this->bulkHash);
                $this->logger->addDebug( "end bulk action at {time}", [ "time" => microtime(true) ]);
                $this->logger->addInfo("Batch[ jobNb => {batchNb} ] Processed docs for current jobNb [{processedDocs} / {batchSize}] - Bulk op for {bulkDocNb}doc(s) - Memory usage {mem}Mo",
                    [
                    'processedDocs' => $this->docsProcessed,
                    'bulkDocNb' => (count($this->bulkHash['body'])/2),
                    'mem' => (memory_get_usage(true)/1048576),
                    'batchNb' =>
                      isset($this->migrationHash['documentsBatch']['batchNb']) ? $this->migrationHash['documentsBatch']['batchNb'] : 1,
                    'batchSize' =>
                      isset($this->migrationHash['documentsBatch']['batchSize']) ? $this->migrationHash['documentsBatch']['batchSize'] : $this->totalDocNb
                    ]
                );
                $this->bulkHash = $this->migrationHash['to'];
            }
        }
    }
    /**
    * Switches alias from Source server to Target source
    *
    * @param string $alias
    *
    * @return void
    */
    public function switchAlias($alias) {
        $fromParams['body'] = array(
            'actions' => array(
                array(
                    'remove' => array(
                        'index' => $this->migrationHash['from']['index'],
                        'alias' => $alias
                        )
                    )
                )
        );
        $toParams['body'] = array(
            'actions' => array(
                array(
                    'add' => array(
                        'index' => $this->migrationHash['to']['index'],
                        'alias' => $alias
                        )
                    )
                )
        );
        $targetIndicesWithAliasResponse = $this->ESClientTarget->indices()->getAlias(['index'=>'*','name' => $alias]);
        $targetIndicesWithAlias = array_keys($targetIndicesWithAliasResponse);
        if (!in_array($this->migrationHash['to']['index'], $targetIndicesWithAlias)) {
            $this->ESClientTarget->indices()->updateAliases($toParams);
            $this->logger->addInfo(
                " Added alias {alias} to {index} ",
                [
                    'alias' => $alias,
                    'index' => $this->migrationHash['to']['index']
                ]
            );
        }
        $sourceIndicesWithAliasResponse = $this->ESClientSource->indices()->getAlias(['index'=>'*','name' => $alias]);
        $sourceIndicesWithAlias = array_keys($sourceIndicesWithAliasResponse);
        if (in_array($this->migrationHash['from']['index'], $sourceIndicesWithAlias)) {
            $this->ESClientSource->indices()->updateAliases($fromParams);
            $this->logger->addInfo(
                " Removed alias {alias} from {index} ",
                [
                    'alias' => $alias,
                    'index' => $this->migrationHash['from']['index'],

                ]
            );
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
            "search_type" => "scan"
            ];
        $searchDefaultHash = [
            "scroll" => "30s"
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

    /**
     * Defines if the given doc is a list of doc and not a signle one
     * Because ELS does not allow int a property
     *
     * @param array $docHash the doc hash given by the user callback func
     *
     * @return bool true if the given doc has to be processed as a list of docs
     */
    private function shouldSplittDoc($docHash)
    {
        $keyList = array_keys($docHash);
        foreach($keyList as $key) {
            if(!is_numeric($key)) return false;
        }
        return true;
    }
}
