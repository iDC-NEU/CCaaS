//
// Created by 周慰星 on 11/8/22.
//

#include "tools/context.h"
#include "tools/tinyxml2.h"

namespace Taas {

    void Context::GetTaaSServerInfo(const std::string& config_file_path){
        tinyxml2::XMLDocument doc;
        doc.LoadFile(config_file_path.c_str());
        auto* root=doc.RootElement();

        tinyxml2::XMLElement* server = root->FirstChildElement("server_type");
        server_type = static_cast<ServerMode>(std::stoull(server->GetText()));

        tinyxml2::XMLElement* server_mode = root->FirstChildElement("taas_server_mode");
        taas_mode = static_cast<TaasMode>(std::stoull(server_mode->GetText()));

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
        tinyxml2::XMLElement* merge_thread_num = root->FirstChildElement("worker_thread_num");
        kWorkerThreadNum = std::stoull(merge_thread_num->GetText());

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

    std::string Context::Print() {
        std::string res = "";
        res += "Config Info:\n \tServerIp:\n";
        int cnt = 0;
        for(const auto& i : kServerIp) {
            res += "\t \t ID: " + std::to_string(cnt++) + ", IP: " + i.c_str() + "\n";
        }
        res += "\t ServerNum: "+ std::to_string(kTxnNodeNum) + "\n\t txn_node_ip_index: "
                + std::to_string(txn_node_ip_index) + "\n\t EpochSize_us: " + std::to_string(kEpochSize_us) + "\n";
        res += "\t CacheLength: " + std::to_string(kCacheMaxLength) + "\n";
        res += "\t WorkerNum: " + std::to_string(kWorkerThreadNum) + "\n\t DurationTime_us: " + std::to_string(kDurationTime_us) + "\n";
        res += "\t TestClientNum: " + std::to_string(kTestClientNum) + "\n\t TestKeyRange: "
                + std::to_string(kTestKeyRange) + "\n\t TestTxnOpNum: " + std::to_string(kTestTxnOpNum) + "\n";
        res += "\t SycnStart: " + std::to_string(is_sync_start) + "\n";
        return res;
    }

    void Context::GetStorageInfo(const std::string& config_file_path){
        tinyxml2::XMLDocument doc;
        doc.LoadFile(config_file_path.c_str());
        auto* root=doc.RootElement();

        tinyxml2::XMLElement* tikv = root->FirstChildElement("is_tikv_enable");
        is_tikv_enable = std::stoull(tikv->GetText());
        tinyxml2::XMLElement *ip_port= root->FirstChildElement("tikv_ip");
        auto tikv_ip=ip_port->GetText();
        kTiKVIP = std::string(tikv_ip);

        tinyxml2::XMLElement* leveldb = root->FirstChildElement("is_leveldb_enable");
        is_leveldb_enable = std::stoull(leveldb->GetText());
        tinyxml2::XMLElement *leveldb_ip_port= root->FirstChildElement("tikv_ip");
        auto leveldb_ip = leveldb_ip_port->GetText();
        kLevevDBIP = std::string(leveldb_ip);

        tinyxml2::XMLElement* hbase = root->FirstChildElement("is_hbase_enable");
        is_hbase_enable = std::stoull(hbase->GetText());
        tinyxml2::XMLElement *hbase_ip_port= root->FirstChildElement("hbase_ip");
        auto hbase_ip=hbase_ip_port->GetText();
        kHbaseIP = std::string(hbase_ip);
    }
}
