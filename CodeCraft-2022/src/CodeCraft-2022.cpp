#include <fstream>
#include <iomanip>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

void loadData(vector<vector<int>> &qos, vector<vector<int>> &demand, vector<int> &bandwidth,
              vector<string> &serverID, vector<string> &clientID) {
    ifstream qos_file("/data/qos.csv", ios::in);
    int N = 0;
    string line;
    getline(qos_file, line);
    istringstream sin(line);
    string field;
    getline(sin, field, ',');
    while (getline(sin, field, ',')) {
        clientID.emplace_back(field);
    }
    while (getline(qos_file, line)) {
        qos.push_back({});
        string field;
        istringstream sin(line);
        getline(sin, field, ',');
        serverID.emplace_back(field);
        while (getline(sin, field, ',')) {
            qos[N].emplace_back(atoi(field.c_str()));
        }
        ++N;
    }
    qos_file.close();

    ifstream demand_file("/data/demand.csv", ios::in);
    N = 0;
    getline(demand_file, line);
    while (getline(demand_file, line)) {
        demand.push_back({});
        string field;
        istringstream sin(line);
        getline(sin, field, ',');
        while (getline(sin, field, ',')) {
            demand[N].emplace_back(atoi(field.c_str()));
        }
        ++N;
    }
    demand_file.close();

    ifstream bandwidth_file("/data/site_bandwidth.csv", ios::in);
    getline(bandwidth_file, line);
    while (getline(bandwidth_file, line)) {
        string field;
        istringstream sin(line);
        getline(sin, field, ',');
        getline(sin, field, ',');
        bandwidth.emplace_back(atoi(field.c_str()));
        ++N;
    }
    bandwidth_file.close();
}

struct serverNode {
    int maxBW;
    int bw;
    string name;
    int calltimes;
    double weight;
    serverNode(int bd, int used, string name, int times = 0, int w = 0)
        : maxBW(bd), bw(used), name(name), calltimes(times), weight(w) {}
};

void resetServerBW(vector<serverNode *> &serverList) {
    for (int j = 0; j < serverList.size(); ++j) {
        serverList[j]->bw = 0;
    }
}

void resetServerW(vector<serverNode *> &serverList, const vector<double> weights) {
    for (int j = 0; j < serverList.size(); ++j) {
        serverList[j]->weight = weights[j];
    }
}

int main() {
    vector<vector<int>> qos;
    vector<vector<int>> demand;
    vector<int> bandwidth;
    vector<string> serverID;
    vector<string> clientID;
    loadData(qos, demand, bandwidth, serverID, clientID);
    int N = clientID.size(), M = serverID.size(), T = demand.size();
    ifstream config_file("/data/config.ini", ios::in);
    string line;
    getline(config_file, line);
    getline(config_file, line);
    string tmp = line.substr(15, line.size() - 15);
    int qos_constraint = atoi(tmp.c_str());

    struct cmp {
        bool operator()(const serverNode *m, const serverNode *n) {
            if (m->maxBW == m->bw) {
                return true;
            }
            return m->bw > n->bw;
        }
    };

    unordered_map<string, serverNode *> serverMap;
    vector<serverNode *> serverList(M);
    for (int i = 0; i < M; ++i) {
        serverList[i] = new serverNode(bandwidth[i], 0, serverID[i]);
        serverMap[serverID[i]] = serverList[i];
    }

    vector<vector<serverNode *>> clientList(N);
    for (int i = 0; i < N; ++i) {
        for (int j = 0; j < M; ++j) {
            if (qos[j][i] < qos_constraint) {
                clientList[i].emplace_back(serverList[j]);
                ++serverList[j]->calltimes;
            }
        }
    }

    double maxWeight = 100;
    int maxValue = 0;
    for (auto it : serverList) {
        if (it->calltimes != 0) {
            maxValue = max(maxValue, it->maxBW / it->calltimes);
        }
    }
    vector<double> weights(M, 0);
    for (int i=0;i<M;++i) {
        if (serverList[i]->calltimes != 0) {
            serverList[i]->weight = serverList[i]->maxBW * maxWeight / serverList[i]->calltimes / maxValue;
            weights[i] = serverList[i]->weight;
        }
    }

    vector<vector<vector<pair<string, int>>>> res(T, vector<vector<pair<string, int>>>(N));
    for (int i = 0; i < T; ++i) {
        resetServerW(serverList, weights);
        bool isOutrange = false;
        int iter = 0;
        while (isOutrange || iter < 100) {
            resetServerBW(serverList);
            isOutrange = false;
            vector<vector<pair<string, int>>> resT(N);
            for (int j = 0; j < N; ++j) {
                int require = demand[i][j];
                double totalW = 0;
                for (auto it : clientList[j]) {
                    totalW += it->weight;
                }
                int tmp = require;
                for (int k = 0; k < clientList[j].size(); ++k) {
                    int distribute;
                    if (k == clientList[j].size() - 1) {
                        distribute = tmp;
                    } else {
                        distribute = require * clientList[j][k]->weight / totalW;
                    }
                    clientList[j][k]->bw += distribute;
                    if (clientList[j][k]->bw > clientList[j][k]->maxBW) {
                        clientList[j][k]->weight *= 0.9;
                        isOutrange = true;
                        break;
                    }
                    tmp -= distribute;
                    if (distribute != 0) {
                        resT[j].push_back({clientList[j][k]->name, distribute});
                    }
                }
                if (isOutrange) {
                    break;
                }
            }
            if (!isOutrange) {
                ++iter;
                res[i] = resT;
                double avgBW = 0;
                int num = 0;
                string mostName = serverList[0]->name, leastName = serverList[0]->name;
                for (auto it : serverList) {
                    if (it->weight != 0) {
                        avgBW += it->bw;
                        ++num;
                        if (it->bw > serverMap[mostName]->bw) {
                            mostName = it->name;
                        }
                        if (it->bw < serverMap[leastName]->bw) {
                            leastName = it->name;
                        }
                    }
                }
                avgBW /= num;
                int base = 3;
                for (auto it : serverList) {
                    if (it->weight != 0) {
                        double rate = it->bw / avgBW;
                        if (rate > 1.3 || rate < 0.7) {
                            it->weight -= base * (rate - 1);
                            if (it->weight < 1) {
                                it->weight = 1;
                            }
                        }
                    }
                }
            }
        }
    }

    ofstream solution("/output/solution.txt", ios::trunc);
    for (int i = 0; i < T; ++i) {
        for (int j = 0; j < N; ++j) {
            int tmp = 0;
            solution << clientID[j] << ":";
            for (int k = 0; k < res[i][j].size(); ++k) {
                tmp += res[i][j][k].second;
                if (k == res[i][j].size() - 1) {
                    solution << "<" << res[i][j][k].first << "," << res[i][j][k].second << ">";
                } else {
                    solution << "<" << res[i][j][k].first << "," << res[i][j][k].second << ">,";
                }
            }
            if (i != T - 1 || j != N - 1) {
                solution << endl;
            }
        }
    }
    solution.close();

    return 0;
}
