//
// Created by 周慰星 on 11/8/22.
//

#include "tools/context.h"
#include "tools/tinyxml2.h"

namespace Taas {

    void TaasContext::GetTaaSServerInfo(const std::string& config_file_path){
        tinyxml2::XMLDocument doc;
        doc.LoadFile(config_file_path.c_str());
        auto* root=doc.RootElement();

        tinyxml2::XMLElement* server = root->FirstChildElement("server_type");
        server_type = static_cast<ServerMode>(std::stoull(server->GetText()));

        tinyxml2::XMLElement* server_mode = root->FirstChildElement("taas_server_mode");
        taasMode = static_cast<TaasMode>(std::stoull(server_mode->GetText()));

        tinyxml2::XMLElement* server_num = root->FirstChildElement("txn_node_num");
        kTxnNodeNum= std::stoull(server_num->GetText());
        tinyxml2::XMLElement* txn_node_ip_index_xml = root->FirstChildElement("txn_node_ip_index");
        txn_node_ip_index=std::stoull(txn_node_ip_index_xml->GetText()) ;
        tinyxml2::XMLElement *index_element = root->FirstChildElement("txn_node_ip");
        while (index_element){
            tinyxml2::XMLElement *ip_port = index_element->FirstChildElement("txn_ip");
            const char* content;
            while(ip_port){
                content = ip_port->GetText();
                std::string temp(content);
                kServerIp.push_back(temp);
                ip_port=ip_port->NextSiblingElement();

            }
            index_element = index_element->NextSiblingElement();
        }

        tinyxml2::XMLElement* sync_start = root->FirstChildElement("sync_start");
        is_sync_start = std::stoull(sync_start->GetText());
        tinyxml2::XMLElement* epoch_size_us = root->FirstChildElement("epoch_size_us");
        kEpochSize_us= std::stoull(epoch_size_us->GetText());
        tinyxml2::XMLElement* cachemaxlength = root->FirstChildElement("cache_max_length");
        kCacheMaxLength = std::stoull(cachemaxlength->GetText());

        tinyxml2::XMLElement* merge_thread_num = root->FirstChildElement("merge_thread_num");
        kMergeThreadNum = std::stoull(merge_thread_num->GetText());
        tinyxml2::XMLElement* commit_thread_num = root->FirstChildElement("commit_thread_num");
        kCommitThreadNum = std::stoull(commit_thread_num->GetText());
        tinyxml2::XMLElement* epoch_txn_thread_num = root->FirstChildElement("epoch_txn_thread_num");
        kEpochTxnThreadNum = std::stoull(epoch_txn_thread_num->GetText());
        tinyxml2::XMLElement* epoch_message_thread_num = root->FirstChildElement("epoch_message_thread_num");
        kEpochMessageThreadNum = std::stoull(epoch_message_thread_num->GetText());

        tinyxml2::XMLElement* duration_time = root->FirstChildElement("duration_time_us");
        kDurationTime_us = std::stoull(duration_time->GetText());
        tinyxml2::XMLElement* client_num = root->FirstChildElement("test_client_num");
        kTestClientNum = std::stoull(client_num->GetText());
        tinyxml2::XMLElement* key_range = root->FirstChildElement("test_key_range");
        kTestKeyRange = std::stoull(key_range->GetText());
        tinyxml2::XMLElement* test_txn_op_num = root->FirstChildElement("test_txn_op_num");
        kTestTxnOpNum = std::stoull(test_txn_op_num->GetText());

        /** Get glog path */
        tinyxml2::XMLElement *glog_path = root->FirstChildElement("glog_path");
        glog_path_ = std::string(glog_path->GetText());

        auto* mode_size_t = root->FirstChildElement("print_mode_size");
        print_mode_size = std::stoull(mode_size_t->GetText());

    }

    std::string TaasContext::Print() {
        std::string res = "";
        res += "Config Info:\n \tServerIp:\n";
        int cnt = 0;
        for(const auto& i : kServerIp) {
            res += "\t \t ID: " + std::to_string(cnt++) + ", IP: " + i.c_str() + "\n";
        }
        res += "\t ServerNum: "+ std::to_string(kTxnNodeNum) + "\n\t txn_node_ip_index: "
                + std::to_string(txn_node_ip_index) + "\n\t EpochSize_us: " + std::to_string(kEpochSize_us) + "\n";
        res += "\t CacheLength: " + std::to_string(kCacheMaxLength) + "\n";
        res += "\t MergeThreadNum: " + std::to_string(kMergeThreadNum) + "\n\t DurationTime_us: " + std::to_string(kDurationTime_us) + "\n";
        res += "\t TestClientNum: " + std::to_string(kTestClientNum) + "\n\t TestKeyRange: "
                + std::to_string(kTestKeyRange) + "\n\t TestTxnOpNum: " + std::to_string(kTestTxnOpNum) + "\n";
        res += "\t SycnStart: " + std::to_string(is_sync_start) + "\n";
        return res;
    }

    void StorageContext::GetStorageInfo(const std::string& config_file_path){
        tinyxml2::XMLDocument doc;
        doc.LoadFile(config_file_path.c_str());
        auto* root=doc.RootElement();

        tinyxml2::XMLElement* mot = root->FirstChildElement("is_mot_enable");
        is_mot_enable = std::stoull(mot->GetText());
        tinyxml2::XMLElement* mot_thread_num = root->FirstChildElement("mot_thread_num");
        kMOTThreadNum = std::stoull(mot_thread_num->GetText());

        tinyxml2::XMLElement* tikv = root->FirstChildElement("is_tikv_enable");
        is_tikv_enable = std::stoull(tikv->GetText());
        tinyxml2::XMLElement *ip_port= root->FirstChildElement("tikv_ip");
        auto tikv_ip=ip_port->GetText();
        kTiKVIP = std::string(tikv_ip);
        tinyxml2::XMLElement* tikv_thread_num = root->FirstChildElement("tikv_thread_num");
        kTikvThreadNum = std::stoull(tikv_thread_num->GetText());

        tinyxml2::XMLElement* leveldb = root->FirstChildElement("is_leveldb_enable");
        is_leveldb_enable = std::stoull(leveldb->GetText());
        tinyxml2::XMLElement *leveldb_ip_port= root->FirstChildElement("leveldb_ip");
        auto leveldb_ip = leveldb_ip_port->GetText();
        kLevelDBIP = std::string(leveldb_ip);
        tinyxml2::XMLElement* leveldb_thread_num = root->FirstChildElement("leveldb_thread_num");
        kLeveldbThreadNum = std::stoull(leveldb_thread_num->GetText());

        tinyxml2::XMLElement* hbase = root->FirstChildElement("is_hbase_enable");
        is_hbase_enable = std::stoull(hbase->GetText());
        tinyxml2::XMLElement *hbase_ip_port= root->FirstChildElement("hbase_ip");
        auto hbase_ip=hbase_ip_port->GetText();
        kHbaseIP = std::string(hbase_ip);
        tinyxml2::XMLElement* hbase_thread_num = root->FirstChildElement("hbase_thread_num");
        kHbaseTxnThreadNum = std::stoull(hbase_thread_num->GetText());

    }

    void MultiModelContext::GetMultiModelInfo(const std::string &config_file_path) {
        tinyxml2::XMLDocument doc;
        doc.LoadFile("../MultiModelConfig.xml");
        tinyxml2::XMLElement *root=doc.RootElement();

        tinyxml2::XMLElement *multimodel_client = root->FirstChildElement("multimodel_client");
        auto multimodel_clients = multimodel_client->GetText();
        kMultiModelClientIP = std::string(multimodel_clients);

        tinyxml2::XMLElement *taas_ip_port = root->FirstChildElement("taas_ip");
        auto taas_ip = taas_ip_port->GetText();
        kTaasIP = std::string(taas_ip);

        tinyxml2::XMLElement* use_nebula = root->FirstChildElement("use_nebula");
        isUseNebula = std::stoull(use_nebula->GetText());

        tinyxml2::XMLElement *nebula_ip_port = root->FirstChildElement("nebula_ip");
        auto nebula_ip = nebula_ip_port->GetText();
        kNebulaIP = std::string(nebula_ip);

        tinyxml2::XMLElement *nebula_user = root->FirstChildElement("nebula_user");
        auto nebula_users = nebula_user->GetText();
        kNebulaUser = std::string(nebula_users);

        tinyxml2::XMLElement *nebula_pwd = root->FirstChildElement("nebula_pwd");
        auto nebula_pwds = nebula_pwd->GetText();
        kNebulaPwd = std::string(nebula_pwds);

        tinyxml2::XMLElement *nebula_space = root->FirstChildElement("nebula_space");
        auto nebula_spaces = nebula_space->GetText();
        kNebulaSpace = std::string(nebula_spaces);


        tinyxml2::XMLElement* use_mot = root->FirstChildElement("use_mot");
        isUseMot = std::stoull(use_mot->GetText());

        tinyxml2::XMLElement *mot_ip_port = root->FirstChildElement("mot_ip");
        auto mot_ip = mot_ip_port->GetText();
        kMOTIP = std::string(mot_ip);

        tinyxml2::XMLElement *mot_dsnnames = root->FirstChildElement("mot_dsnname");
        auto mot_dsnname = mot_dsnnames->GetText();
        kMOTDsnName = std::string(mot_dsnname);

        tinyxml2::XMLElement *mot_dsnuids = root->FirstChildElement("mot_dsnuid");
        auto mot_dsnuid = mot_dsnuids->GetText();
        kMOTDsnUid = std::string(mot_dsnuid);

        tinyxml2::XMLElement *mot_dsnpwd = root->FirstChildElement("mot_dsnpwd");
        auto mot_dsnpwds = mot_dsnpwd->GetText();
        kMOTDsnPwd = std::string(mot_dsnpwds);


        tinyxml2::XMLElement* test_mode = root->FirstChildElement("test_mode");
        kTestMode = static_cast<TestMode>(std::stoull(test_mode->GetText()));

        tinyxml2::XMLElement* is_generate_txn = root->FirstChildElement("is_load_data");
        isLoadData = std::stoull(is_generate_txn->GetText());

        tinyxml2::XMLElement* record_count = root->FirstChildElement("record_count");
        kRecordCount = std::stoull(record_count->GetText());

        tinyxml2::XMLElement* server_num = root->FirstChildElement("txn_num");
        kTxnNum =  std::stoull(server_num->GetText());

        tinyxml2::XMLElement* write = root->FirstChildElement("write");
        kWriteNum =  std::stoull(write->GetText());

        tinyxml2::XMLElement* read = root->FirstChildElement("read");
        kReadNum =  std::stoull(read->GetText());

        tinyxml2::XMLElement* opnum = root->FirstChildElement("op_num");
        kOpNum =  std::stoull(opnum->GetText());

        tinyxml2::XMLElement *distribution = root->FirstChildElement("distribution");
        auto distribution_s = distribution->GetText();
        kDistribution = std::string(distribution_s);

        tinyxml2::XMLElement* client_threads = root->FirstChildElement("client_threads");
        kClientNum =  std::stoull(client_threads->GetText());


    }
}
