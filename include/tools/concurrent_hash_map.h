//
// Created by 周慰星 on 11/8/22.
//
#ifndef TAAS_CONCURRENT_HASH_MAP_H
#define TAAS_CONCURRENT_HASH_MAP_H

#pragma once

#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <unistd.h>

namespace Taas {
    template<typename key, typename value, typename pointer>
    class concurrent_crdt_unordered_map {
    public:
        typedef typename std::unordered_map<key, value>::iterator map_iterator;
        typedef typename std::unordered_map<key, value>::size_type size_type;

        bool insert(const key &k, const value &v, pointer &ptr) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            bool result = true;
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if (iter == _map_temp.end()) {
                _map_temp[k] = v;
                ptr = "0";
                result = true;
            } else {
                if (iter->second > v) {
                    ptr = _map_temp[k];
                    _map_temp[k] = v;
                    result = true;
                }
                else if(iter->second == v){
                    ptr = "0";
                    result = true;
                }
                else{
                    ptr = v;
                    result = false;
                }
            }
            lock.unlock();
            return result;
        }

        void insert(const key &k, const value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            _map_temp[k] = v;
        }


        void remove(const key &k, const value &v) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                if (iter->second == v) {
                    //if the abort txn has insert row and has not been modifid by ohters
                    //then remove it from map;or keep it
                    _map_temp.erase(iter);
                }
            }
            lock.unlock();
        }

        void remove(const key &k) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                _map_temp.erase(iter);
            }
            lock.unlock();
        }

        void clear() {
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
            }
            for(uint64_t i = 0; i < _N; i ++){
                _map[i].clear();
            }
        }

        void unsafe_clear() {
            for(uint64_t i = 0; i < _N; i ++){
                _map[i].clear();
            }
        }

        bool contain(const key &k, const value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
            }
            return false;
        }

        bool contain(const key &k){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                return true;
            }
            return false;
        }

        bool unsafe_contain(const key &k, value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
            }
            return false;
        }

        size_type size() {
            size_type ans = 0;
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
                ans += _map[i].size();
            }
            return ans;
        }

        bool getValue(std::vector<key> &keys, std::vector<value> &values) {
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
            }
            for(uint64_t i = 0; i < _N; i ++){
                for(auto p : _map[i]) {
                    keys.push_back(p.first);
                    values.push_back(p.second);
                }
            }
            return true;
        }

    protected:
        inline std::unordered_map<key, value>& GetMapRef(const key k){ return _map[(_hash(k) % _N)]; }
        inline std::unordered_map<key, value>& GetMapRef(const key k) const { return _map[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) { return _mutex[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) const {return _mutex[(_hash(k) % _N)]; }

    private:
        const static uint64_t _N = 101;//101 337 599 733 911 1217 12281 122777 prime
        std::hash<key> _hash;
        std::unordered_map<key, value> _map[_N];
        std::mutex _mutex[_N];
    };

    template<typename key, typename value>
    class concurrent_unordered_map {
    public:
        typedef typename std::unordered_map<key, value>::iterator map_iterator;
        typedef typename std::unordered_map<key, value>::size_type size_type;

        void insert(const key &k, const value &v) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            _map_temp[k] = v;
        }

        void remove(const key &k, const value &v) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock( GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                if (iter->second == v) {
                    //if the abort txn has insert row and has not been modifid by ohters
                    //then remove it from map;or keep it
                    _map_temp.erase(iter);
                }
            }
            lock.unlock();
        }

        void remove(const key &k) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock( GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if (iter != _map_temp.end()) {
                _map_temp.erase(iter);
            }
            lock.unlock();
        }

        void clear() {
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
            }
            for(uint64_t i = 0; i < _N; i ++){
                _map[i].clear();
            }
        }

        void unsafe_clear() {
            for(uint64_t i = 0; i < _N; i ++){
                _map[i].clear();
            }
        }

        bool contain(key &k, value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
            }
            return false;
        }

        bool getValue(const key &k, value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                v = _map_temp[k];
                return true;
            }
            v = value();
            return false;
        }

        bool try_lock(const key &k, value &v) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(_map_temp[k] == v) { /// locked already by itself
                    return true;
                }
                else if(_map_temp[k] == "" || _map_temp[k] == "0"){
                    _map_temp[k] = v;
                    return true;
                }
                else { /// locked already by others
                    return false;
                }
            }
            _map_temp[k] = v;
            return true;
        }


        value unlock(const key &k, value &v) {
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(GetMutexRef(k));
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(_map_temp[k] == v) { /// locked already by itself
                    _map_temp[k] = "";
                }
                else if(_map_temp[k] == "" || _map_temp[k] == "0" || _map_temp[k] == "-1"){
                    _map_temp[k] = "";
                }
                else { /// locked already by others
                    /// do nothing
                }
            }
            value tmp = _map_temp[k];
            lock.unlock();
            return tmp;
        }



        bool contain(const key &k){
            std::mutex& _mutex_temp = GetMutexRef(k);
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            std::unique_lock<std::mutex> lock(_mutex_temp);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                return true;
            }
            return false;
        }

        bool unsafe_contain(const key &k, value &v){
            std::unordered_map<key, value>& _map_temp = GetMapRef(k);
            map_iterator iter = _map_temp.find(k);
            if(iter != _map_temp.end()){
                if(iter->second == v){
                    return true;
                }
            }
            return false;
        }

        size_type size() {
            size_type ans = 0;
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
                ans += _map[i].size();
            }
            return ans;
        }

        bool getValue(std::basic_string<char> keys, std::vector<value> &values) {
            for(uint64_t i = 0; i < _N; i ++){
                std::unique_lock<std::mutex> lock(_mutex[i]);
            }
            for(uint64_t i = 0; i < _N; i ++){
                for(auto p : _map[i]) {
                    keys.push_back(p.first);
                    values.push_back(p.second);
                }
            }
            return true;
        }

    protected:
        inline std::unordered_map<key, value>& GetMapRef(const key k){ return _map[(_hash(k) % _N)]; }
        inline std::unordered_map<key, value>& GetMapRef(const key k) const { return _map[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) { return _mutex[(_hash(k) % _N)]; }
        inline std::mutex& GetMutexRef(const key k) const {return _mutex[(_hash(k) % _N)]; }

    private:
        const static uint64_t _N = 101;//1217 12281 122777 prime
        std::hash<key> _hash;
        std::unordered_map<key, value> _map[_N];
        std::mutex _mutex[_N];
    };
}


#endif //TAAS_CONCURRENT_HASH_MAP_H
