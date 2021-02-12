#include <fstream>
#include <iostream>

using namespace std;

int main(int argc, char *argv[]) {
    string ifilename, line;

    if(argc < 2) {
        cout << "Input index file name to convert: " << endl;
        cin >> ifilename;
        cout << endl;
    }
    else {
        ifilename = argv[1];
    }

    string ofilename = "converted_" + ifilename.substr(0, ifilename.find('.')) + ".json";
    ifstream ifile(ifilename.c_str());
    ofstream ofile(ofilename.c_str());
    

    while(getline(ifile, line)) {
        if(line.size() < 2)
            break;
        
        int index0, index1, uid;

        index0 = line.find(':') + 2;
        index1 = line.find(',');
        string s_uid = line.substr(index0, index1-index0);
        uid = stoi(s_uid);

        ofile << "{ \"index\" : { \"_index\": \"images\", \"_id\" : \"";
        ofile << uid << "\" } }" << endl;
        ofile << line << endl;
    }
    ifile.close();
    ofile.close();
}