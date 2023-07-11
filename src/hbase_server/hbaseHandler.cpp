//
// Created by user on 23-7-3.
//

#include "hbase_server/hbaseHandler.h"

#include <iostream>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hadoop::hbase::thrift2;

CHbaseHandler::CHbaseHandler() :
        socket((TSocket*)NULL), transport((TBufferedTransport*)NULL), protocol((TBinaryProtocol*)NULL), /*client((THBaseServiceClient*)NULL),*/ hbaseServicePort(9090),isConnected(false)
{

}

CHbaseHandler::~CHbaseHandler()
{
    if (isConnected) {
        disconnect();
    }
}

/**
* Connect Hbase.
*
*/
bool CHbaseHandler::connect()
{
    if (isConnected)
    {
        cerr << "Already connected, don't need to connect it again" << endl;
        return true;
    }
    try
    {
        socket.reset(new TSocket(hbaseServiceHost, hbaseServicePort));
        transport.reset(new TBufferedTransport(socket));
        protocol.reset(new TBinaryProtocol(transport));
        //client = make_shared<THBaseServiceClient>(protocol);
        transport->open();
    }
    catch (const TException &tx)
    {
        cerr << "Connect Hbase error : " << tx.what() << endl;
        return false;
    }

    isConnected = true;
    return isConnected;
}

/**
 * Connect Hbase.
 *
 * @param host [IN] The host ip.
 * @param port [IN] The port number.
 * @return True for successfully connect Hbase, false otherwise.
 */
bool CHbaseHandler::connect(const std::string &host, int port)
{
    hbaseServiceHost = host;
    hbaseServicePort = port;

    return connect();
}

/**
* Disconnect from Hbase.
*
*/
bool CHbaseHandler::disconnect()
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't disconnect from it" << endl;
        return false;
    }

    if (NULL != transport)
    {
        try
        {
            transport->close();
        }
        catch (const TException &tx)
        {
            cerr << "Disconnect Hbase error : " << tx.what() << endl;
            return false;
        }
    }
    else
    {
        return false;
    }

    isConnected = false;
    return true;
}

/**
* Put a row into Hbase.
 *
* @param tableName [IN] The table's name.
* @param rowKey 	[IN] The rowKey.
* @param family 	[IN] The family.
* @param qualifier [IN] The qualifier.
* @param rowValue 	[IN] The rowValue.
* @return True for successfully put a row into Hbase, false otherwise.
*/
bool CHbaseHandler::putRow(const string &tableName,
                           const string &rowKey,
                           const std::string &family,
                           const std::string &qualifier,
                           const string &rowValue)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't put row" << endl;
        return false;
    }

    try
    {
        THBaseServiceClient client(protocol);
        TResult tresult;
        //TGet get;
        std::vector<TPut> puts;
        TPut put;
        std::vector<TColumnValue> cvs;
        put.__set_row(rowKey);
        TColumnValue tcv;
        tcv.__set_family(family);
        tcv.__set_qualifier(qualifier);
        tcv.__set_value(rowValue);
        cvs.insert(cvs.end(), tcv);
        put.__set_columnValues(cvs);
        //puts.insert(puts.end(), put);
        //client.putMultiple(table, puts);
        client.put(tableName, put);
    }
    catch(const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }

    return true;
}

/**
 * Get a row from Hbase.
 *
 * @param tableName [IN] The table's name.
 * @param rowKey 	[IN] The rowKey.
 * @param resultStr [OUT] The string which stores result.
 * @return True for successfully get a row from Hbase, false otherwise.
 */
bool CHbaseHandler::getRow(const std::string &tableName, const std::string &rowKey, std::string &resultStr)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't read data from it" << endl;
        return false;
    }

    try
    {
        THBaseServiceClient client(protocol);
        TResult tresult;
        TGet get;

        std::vector<TColumnValue> cvs;
        get.__set_row(rowKey);
        //bool be = client.exists(tableName, get);
        client.get(tresult, tableName, get);
        vector<TColumnValue> list = tresult.columnValues; //get column values
        std::vector<TColumnValue>::const_iterator iter = list.begin();
        std::vector<TColumnValue>::const_iterator iterEnd = list.end();
        resultStr.clear();
        for(; iter != iterEnd; ++iter)
        {
            resultStr = resultStr + "$$" + (*iter).family + "$$" + (*iter).qualifier + "$$" + (*iter).value + "$$" + boost::lexical_cast<std::string>((*iter).timestamp) + '\n';
        }
    }
    catch (const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }

    return true;
}

/**
 * Get rows from Hbase.
 *
 * @param tableName [IN] The table's name.
 * @param rowKeyVec [IN] The rowKey's vector.
 * @param resultStr [OUT] The string which stores result.
 * @return True for successfully get a row from Hbase, false otherwise.
 */
bool CHbaseHandler::getRows(const std::string &tableName, const std::vector<std::string> &rowKeyVec, std::string &resultStr)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't read data from it" << endl;
        return false;
    }

    try
    {
        resultStr.clear();
        std::vector<std::string>::const_iterator citer = rowKeyVec.begin();
        std::vector<std::string>::const_iterator citerEnd = rowKeyVec.end();
        for(; citer != citerEnd; ++citer)
        {
            std::string subResultStr;
            getRow(tableName, *citer, subResultStr);
            resultStr += subResultStr;
        }
    }
    catch (const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }

    return true;
}

/**
 * Get rows from Hbase by timeRange.
 *
 * @param tableName 		[IN] The tableName.
 * @param startTimestamp 	[IN] The startTimestamp.
 * @param endTimestamp 		[IN] The endTimestamp.
 * @param resultStr 		[OUT] The resultStr.
 * @return True for successfully get rows from Hbase, false otherwise.
 */
bool CHbaseHandler::getRowsByTime(const std::string &tableName, int64_t startTimestamp, int64_t endTimestamp, std::string &resultStr)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't read data from it" << endl;
        return false;
    }

    try
    {
        //构造TTimeRange对象
        TTimeRange tr;
        tr.__set_minStamp(startTimestamp);
        tr.__set_maxStamp(endTimestamp);

        THBaseServiceClient client(protocol);
        TResult tresult;
        TScan scan;
        vector<TResult> vecRes;
        std::string startRow("");	//从头扫描
        scan.__set_timeRange(tr);	//只提取时间间隔(1s)内的数据
        scan.__set_startRow(startRow);
        int scanId = client.openScanner(tableName, scan);
        client.getScannerRows(vecRes, scanId, 100000); //写入结果,默认读100000条
        client.closeScanner(scanId);

        std::vector<TResult>::iterator iter = vecRes.begin();
        std::vector<TResult>::iterator iterEnd = vecRes.end();
        resultStr.clear();
        for(;iter != iterEnd; ++iter)
        {
            vector<TColumnValue> list = (*iter).columnValues;
            std::vector<TColumnValue>::const_iterator citer = list.begin();
            std::vector<TColumnValue>::const_iterator citerEnd = list.end();
            for(; citer != citerEnd; ++citer)
            {
                //printf("%s, %s,%s\n",(*citer).family.c_str(),(*citer).qualifier.c_str(),(*citer).value.c_str());
                resultStr = resultStr + (*citer).family + "$$" + (*citer).qualifier + "$$" + (*citer).value + "$$" + boost::lexical_cast<std::string>((*citer).timestamp) + '\n';
            }
        }
    }
    catch (const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }
    return true;
}

/**
 * Get allRows from a table in Hbase, which is similar to 'scan' command in hbase shell, get 100000 rows by default.
 *
 * @param tableName [IN] The table's name.
 * @param resultStr [OUT] The string which stores result.
 * @return True for successfully get all row from Hbase, false otherwise.
 */
bool CHbaseHandler::getAllRows(const std::string &tableName, std::string &resultStr)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't read data from it" << endl;
        return false;
    }

    try
    {
        THBaseServiceClient client(protocol);
        TResult tresult;
        TScan scan;
        vector<TResult> vecRes;
        std::string startRow("");	//从头扫描
        scan.__set_startRow(startRow);
        int scanId = client.openScanner(tableName, scan);
        client.getScannerRows(vecRes, scanId, 100000); //写入结果
        client.closeScanner(scanId);

        std::vector<TResult>::iterator iter = vecRes.begin();
        std::vector<TResult>::iterator iterEnd = vecRes.end();
        resultStr.clear();
        for(;iter != iterEnd; ++iter)
        {
            vector<TColumnValue> list = (*iter).columnValues;
            std::vector<TColumnValue>::const_iterator citer = list.begin();
            std::vector<TColumnValue>::const_iterator citerEnd = list.end();
            for(; citer != citerEnd; ++citer)
            {
                //printf("%s, %s,%s\n",(*citer).family.c_str(),(*citer).qualifier.c_str(),(*citer).value.c_str());
                resultStr = resultStr + (*citer).family + "$$" + (*citer).qualifier + "$$" + (*citer).value + "$$" + boost::lexical_cast<std::string>((*citer).timestamp) + '\n';
            }
        }
    }
    catch (const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }

    return true;
}

/**
 * Delete single row in Hbase.
 *
 * @param tableName [IN] The table's name.
 * @param rowKey 	[IN] The rowKey.
 * @return True for successfully delete single row in Hbase, false otherwise.
 */
bool CHbaseHandler::delRow(const std::string &tableName, const std::string &rowKey)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't read data from it" << endl;
        return false;
    }

    try
    {
        THBaseServiceClient client(protocol);
        TResult tresult;
        TDelete del;

        del.__set_row(rowKey);
        client.deleteSingle(tableName, del);
    }
    catch (const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }

    return true;
}

/**
 * Delete a bunch of rows in Hbase.
 *
 * @param tableName [IN] The table's name.
 * @param rowKeyVec [IN] The rowKey's vector.
 * @return True for successfully delete a bunch of rows in Hbase, false otherwise.
 */
bool CHbaseHandler::delRows(const std::string &tableName, const std::vector<std::string> &rowKeyVec)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't read data from it" << endl;
        return false;
    }

    try
    {
        std::vector<std::string>::const_iterator citer = rowKeyVec.begin();
        std::vector<std::string>::const_iterator citerEnd = rowKeyVec.end();
        for(; citer != citerEnd; ++citer)
        {
            delRow(tableName, *citer);
        }
    }
    catch (const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }

    return true;
}

//获取某个字段的值
/**
 * Get columnValue specified by rowkey in Hbase.
 *
 * @param tableName 	[IN] The table's name.
 * @param rowKey 		[IN] The rowKey.
 * @param columnValue 	[OUT] The columnValue after querying.
 * @return True for successfully delete a bunch of rows in Hbase, false otherwise.
 */
bool CHbaseHandler::getSeqValue(const std::string &tableName, const std::string &rowKey, std::string &columnValue)
{
    if (!isConnected)
    {
        cerr << "Haven't connected to Hbase yet, can't read data from it" << endl;
        return false;
    }

    try
    {
        THBaseServiceClient client(protocol);
        TResult tresult;
        TGet get;

        std::vector<TColumnValue> cvs;
        get.__set_row(rowKey);
        //bool be = client.exists(tableName, get);
        client.get(tresult, tableName, get);
        vector<TColumnValue> list = tresult.columnValues; //get column values
        if(list.size() != 1)
        {
            cerr << "错误:存在多条相同的sequence序列" << endl;
            return false;
        }
//		std::vector<TColumnValue>::const_iterator iter = list.begin();
//		std::vector<TColumnValue>::const_iterator iterEnd = list.end();
//		resultStr.clear();
//		for(; iter != iterEnd; ++iter)
//		{
//			resultStr = resultStr + (*iter).family + ":" + (*iter).qualifier + ":" + (*iter).value + ":";
//		}
        columnValue = list.begin()->value;
    }
    catch (const TException &tx)
    {
        cerr << "error: " << tx.what() << endl;
    }

    return true;
}