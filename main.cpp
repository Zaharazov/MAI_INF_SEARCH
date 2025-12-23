#include <iostream>
#include <string>
#include <vector>
#include <locale>
#include <codecvt>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/oid.hpp>

std::vector<std::string> all_doc_ids;

std::wstring utf8_to_wstr(std::string& str) 
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;

    return conv.from_bytes(str);
}

bool svoy_isalpha(wchar_t c) 
{
    if ((c >= L'A' && c <= L'Z') || (c >= L'a' && c <= L'z')) return true;
    if (c >= 0x0410 && c <= 0x044F) return true;
    if (c == 0x0401 || c == 0x0451) return true;

    return false;
}

wchar_t svoy_tolower(wchar_t c) 
{
    if (c >= L'A' && c <= L'Z') return c + (L'a' - L'A');
    if (c >= 0x0410 && c <= 0x042F) return c + 0x20;
    if (c == 0x0401) return 0x0451;

    return c;
}

std::wstring svoy_stem(std::wstring word) 
{
    if (word.size() <= 3) return word;

    static const std::vector<std::wstring> endings = {L"ing", L"ed", L"es", L"ly", L"s", L"ться", L"ами", L"ями", L"ого", L"его", L"ому", L"ему", L"ыми", L"ими", L"ие", L"ые", L"ов", L"ев", L"ий", L"ия", L"ью", L"ом", L"ем", L"а", L"у", L"е", L"и", L"ы", L"й"};

    for (int i = 0; i < endings.size(); i++)
    {
        std::wstring ending = endings[i];
        int w_len = word.size();
        int e_len = ending.size();

        if (w_len > e_len + 2) {
            bool match = true;
            for (int j = 0; j < e_len; j++) 
            {
                if (word[w_len - e_len + j] != ending[j]) 
                {
                    match = false;
                    break;
                }
            }
            if (match) 
            {
                return word.substr(0, w_len - e_len);
            }
        }
    }

    return word;
}

std::vector<std::wstring> tokenize(std::wstring text) 
{
    std::vector<std::wstring> tokens;
    std::wstring token;

    for (int i = 0; i < text.size(); i++) 
    {
        wchar_t c = text[i];
        
        if (svoy_isalpha(c)) 
        { 
            token += svoy_tolower(c); 
        }
        else if (token.size() > 0) 
        { 
            tokens.push_back(token); 
            token = L""; 
        }
    }

    if (token.size() > 0) 
    {
        tokens.push_back(token);
    }

    return tokens;
}

class SvoyMap 
{
    struct Node 
    { 
        std::wstring key; 
        std::vector<int> value; 
    };

    static const int BUCKET_COUNT = 100003;
    std::vector<Node> buckets[BUCKET_COUNT];

    int get_hash(std::wstring key) 
    {
        unsigned int h = 0;
        for (int i = 0; i < key.size(); i++) 
        {
            h = h * 31 + (unsigned int)key[i];
        }
        return h % BUCKET_COUNT;
    }

public:
    std::vector<int>& operator[](std::wstring key) 
    {
        int idx = get_hash(key);

        for (int i = 0; i < buckets[idx].size(); i++)
        {
            if (buckets[idx][i].key == key) 
            {
                return buckets[idx][i].value;
            }
        }

        buckets[idx].push_back({key, {}});
        return buckets[idx][buckets[idx].size() - 1].value;
    }

    bool contains(std::wstring key)
    {
        int idx = get_hash(key);
        for (int i = 0; i < buckets[idx].size(); i++) 
        {
            if (buckets[idx][i].key == key) 
            {
                return true;
            }
        }
        return false;
    }
};

struct BoolIndex 
{
    SvoyMap index;

    void add(int doc_idx, const std::wstring& text) 
    {
        std::vector<std::wstring> words = tokenize(text);

        for (int i = 0; i < words.size(); i++)
        {
            std::wstring s = svoy_stem(words[i]);
            std::vector<int>& postings = index[s];

            if (postings.size() == 0 || postings[postings.size() - 1] != doc_idx) 
            {
                postings.push_back(doc_idx);
            }
        }
    }

    std::vector<int> get(const std::wstring& term) 
    {
        std::wstring s = svoy_stem(term);

        if (index.contains(s)) 
        {
            return index[s];
        }
        
        return std::vector<int>();
    }
};

std::vector<int> bul_and(std::vector<int>& a, std::vector<int>& b) 
{
    std::vector<int> res;
    int i = 0, j = 0;
    while (i < a.size() && j < b.size()) 
    {
        if (a[i] < b[j]) 
        {
            i++;
        } 
        else if (b[j] < a[i]) 
        {
            j++;
        } 
        else 
        {
            res.push_back(a[i]);
            i++;
            j++;
        }
    }
    return res;
}

std::vector<int> bul_or(std::vector<int>& a, std::vector<int>& b) 
{
    std::vector<int> res;
    int i = 0, j = 0;
    while (i < a.size() || j < b.size()) 
    {
        if (i == a.size() || (b[j] < a[i])) 
        {
            res.push_back(b[j]);
            j++;
        } 
        else if (j == b.size() || (a[i] < b[j])) 
        {
            res.push_back(a[i]);
            i++;
        } 
        else 
        {
            res.push_back(a[i]);
            i++;
            j++;
        }
    }
    return res;
}

std::vector<int> bul_not(std::vector<int>& a, std::vector<int>& b) 
{
    std::vector<int> res;
    int i = 0, j = 0;
    while (i < a.size()) 
    {
        if (j == b.size() || a[i] < b[j]) 
        {
            res.push_back(a[i]);
            i++;
        } 
        else if (a[i] == b[j]) 
        {
            i++;
            j++;
        } 
        else 
        {
            j++;
        }
    }
    return res;
}

std::vector<int> boolean_search(std::wstring query, BoolIndex& idx) 
{
    std::vector<std::wstring> tokens = tokenize(query);
    std::vector<int> result;
    bool first = true;
    std::wstring op = L"and";

    for (int i = 0; i < tokens.size(); i++) 
    {
        std::wstring token = tokens[i];

        if (token == L"and" || token == L"or" || token == L"not") 
        {
            op = token; 
            continue;
        }

        std::vector<int> docs = idx.get(token);

        if (first) 
        { 
            result = docs; 
            first = false; 
        }
        else 
        {
            if (op == L"and") 
            {
                result = bul_and(result, docs);
            }
            else if (op == L"or")
            {
                result = bul_or(result, docs);
            }
            else if (op == L"not") 
            {
               result = bul_not(result, docs); 
            }
        }
    }

    return result;
}

int main() {
    std::locale::global(std::locale("")); 
    std::wcin.imbue(std::locale(""));
    std::wcout.imbue(std::locale(""));

    mongocxx::instance inst{};
    mongocxx::client client{ mongocxx::uri{"mongodb://172.29.32.1:27017"} };
    auto collection = client["local"]["IS_lab2_clean"];

    BoolIndex idx;
    int count = 0;

    auto cursor = collection.find({});
    for (auto&& doc : cursor) 
    {
        if (!doc["_id"] || !doc["clean_text"]) continue;

        all_doc_ids.push_back(doc["_id"].get_oid().value.to_string());
            
        std::string text_utf8 = doc["clean_text"].get_string().value.to_string();
        idx.add(all_doc_ids.size() - 1, utf8_to_wstr(text_utf8));

        if (++count % 5000 == 0) 
        {
            std::cout << "Indexed: " << count << std::endl;
        }
    }

    while (true) 
    {
        std::cout << "\nEnter query or 'exit': ";
        std::wstring query;
        if (!(std::getline(std::wcin, query)) || query == L"exit") 
        {
           break; 
        }
        
        std::vector<int> res = boolean_search(query, idx);

        std::cout << "Found: " << res.size() << " docs." << std::endl;
        for (int i = 0; i < res.size() && i < 5; ++i) 
        {
            std::cout << " - ID: " << all_doc_ids[res[i]] << std::endl;
        }
    }

    return 0;
}