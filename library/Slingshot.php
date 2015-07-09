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

    private $logger;
    private $logLineFormat = "%channel% - %level_name%: %message%";

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
        // Init logger
        $this->logger = new Logger('slingshot');
        $syslogH = new SyslogHandler(null, 'local6');
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
        $this->ESClientSource = new \Elasticsearch\Client(['hosts' => [$hostsConfHash['from']]]);
        if ( !empty($hostsConfHash['to']) &&  $hostsConfHash['from'] != $hostsConfHash['to']) {
            $this->ESClientTarget = new \Elasticsearch\Client(['hosts' => [$hostsConfHash['to']]]);
        }
        else {
            $this->ESClientTarget = &$this->ESClientSource;
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
        $startsAt = microtime(true);
        $this->logger->addInfo("Migration starts at {now}", ['now' => date('Y-m-d H:i:s')]);
        $this->searchQueryHash = $searchQueryHash;
        if (!is_callable($convertDocCallBack)) {
            throw new \Exception('Wrong callback function');
        }
        $this->convertDocCallBack = $convertDocCallBack;
        $this->processMappingChanges();
        $this->processDocumentsMigration();
        // Stats
        $endsAt = microtime(true);
        $execTime = round(($endsAt - $startsAt)/60,3);
        $this->logger->addInfo("Migration ends at {now} in {time}s using {mem}Mo max",
            ['now' => date('Y-m-d H:i:s'), 'time' => $execTime, 'mem' => (memory_get_peak_usage(true)/1048576) ]
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
            $this->logger->addInfo("No Mapping changes requiered for {index}/{type}", $this->migrationHash['to']);
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
                    $bulkAction => [ '_id' => $hitHash['_id']]
                    ];
                $bulkHash['body'][] = $docHash;
                $iDoc++;
                // Bulk index the batch size doc or the remaining doc
                if ( !($iDoc % $bulkBatchSize) || !($totalDocNb-$iDoc)) {
                    $this->logger->addInfo("Bulk op for {bulkDocNb}doc(s) - Memory usage {mem}Mo",
                        [
                            'bulkDocNb' => (count($bulkHash['body'])/2),
                            'mem'       => (memory_get_usage(true)/1048576)
                        ]
                    );
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
