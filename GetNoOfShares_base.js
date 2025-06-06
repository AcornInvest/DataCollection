var NoShare = [];
var target_stock = [];
var ListingDate_stock = [];
var DelistingDate_stock = [];
var code_modified = [];
var target_stock_code =[];
var code_listed = 'A005930'; // 삼성전자. 2000년부터 계속 상장된 회사
var stock_listed;
var NoShare_old = [];
var load_failure_list = [];  // 상폐일, 상장일이 기준일 조건을 충족하는데 종목 조회 실패한 데이터들의 인덱스
var DelistingDate_Error_list = [];	// 상폐일이 시뮬레이션 시작일보다 빠른 데이터들의 인덱스
// Parameters
var LastRebalYear = 99; // 초기값을 이렇게 잡아야 2000년 1월 4일부터 돌릴때 2001년에 처음 동작한다.
var numBusinessDay = 520;  // 시스템 최대는 600, 1년 252일 기준 약간 마진을 둠. 초기 데이터 구축이 아니라 업데이트할때는 수정 필요.

var list_index = 0;
var NumOfStocks = 0;
var len_data = code.length;

function ModifyDate(date){
    var year = date.getFullYear();
    var month = ('0' + (date.getMonth() + 1)).slice(-2); // 월은 0부터 시작하므로 1을 더하고, 2자리로 맞추기 위해 0을 붙여줍니다.
    var day = ('0' + date.getDate()).slice(-2); // 날짜를 2자리로 맞추기 위해 0을 붙여줍니다.
	var formattedDate = year + '-' + month + '-' + day;
	return formattedDate;
}

function removeA(str) {
    return str.replace(/^A/, '');
}

function initialize() {
     for (var i = 0; i < len_data; i++) {
    	code_modified[i] = code[i].substring(0, code[i].length - 1) + '0'; // 코드명 마지막 글자 '0'으로 변경
	}
    for (var i = 0; i < len_data; i++) {
    	ListingDate[i].setHours(0, 0, 0, 0);
	}
    for (var i = 0; i < len_data; i++) {
    	DelistingDate[i].setHours(0, 0, 0, 0);
	}
    StartDate.setHours(0, 0, 0, 0);
    FinalDate.setHours(0, 0, 0, 0);
    stock_listed = IQStock.getStock(code_listed);
    
    while ((ListingDate[list_index] <= StartDate) && (list_index < len_data)){ // 더 최근 날짜가 더 크다
        if (DelistingDate[list_index] >= StartDate) {//DelistingDate 보다 기준일이 더 과거인지 확인          
            var _stock = IQStock.getStock(code_modified[list_index]);                
            if (_stock != null)
            {
                target_stock[NumOfStocks] = _stock;
                target_stock_code[NumOfStocks] = code[list_index]; // target stock의 original 코드명 맵핑
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑                                
                NoShare_old[NumOfStocks] = 0; // NoShare_old 초기화
                NumOfStocks++;
                //logger.info(_stock.code)                
            }
            else
            {
                load_failure_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 들어오는데 종목 조회 실패
            }            
		}
		else {
			DelistingDate_Error_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준 안 맞는 것. spac 확인 필요
        }
        list_index++;        
    }    
    // 상장일이 시뮬레이션 시작일보다 빠른 종목들 target_stock에 코드명 맵핑
}

function onDayClose(now){    
    var thisYear = now.getYear();
    var thisMonth = now.getMonth();
    var thisDate = now.getDate();
    var today = stock_listed.getDate();
    
    // today와 같거나 빠른 상장일의 종목들 등록
    while ((ListingDate[list_index] <= today) && (list_index < len_data)){ // 더 최근 날짜가 더 크다
        if (DelistingDate[list_index] >= StartDate) {//DelistingDate 보다 기준일이 더 과거인지 확인
			var _stock = IQStock.getStock(code_modified[list_index]);                
            if (_stock != null)
            {
                target_stock[NumOfStocks] = _stock;
                target_stock_code[NumOfStocks] = code[list_index]; // target stock의 original 코드명 맵핑
                ListingDate_stock[NumOfStocks] = ListingDate[list_index]; // target stock의 상장일 맵핑
                DelistingDate_stock[NumOfStocks] = DelistingDate[list_index]; // target stock의 상폐일 맵핑      
                NoShare_old[NumOfStocks] = 0; // NoShare_old 초기화
                NumOfStocks++;
            }
            else
            {
                load_failure_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 들어오는데 종목 조회 실패
            }            
		}
        else {
			DelistingDate_Error_list.push(removeA(code[list_index])); //상장일, 상폐일이 기준에 안 맞음. spac 확인 필요
        }
        list_index++;        
    }     

    for (var j=0;  j<NumOfStocks; j++){
        if(ListingDate_stock[j] <= today && DelistingDate_stock[j] >= today){
            NoShare[j] = target_stock[j].getNoOfShare();
        	if (NoShare[j] != NoShare_old[j]){
            //logger.info(ModifyDate(stock_listed.getDate(i)) + ', ' + removeA(target_stock[j].code) + ', o: ' + NoShare_old[j] + ', n: ' + NoShare[j]);
            //logger.info(ModifyDate(stock_listed.getDate()) + ', ' + removeA(target_stock_code[j]) + ', o: ' + NoShare_old[j] + ', n: ' + NoShare[j]);
            logger.info(removeA(target_stock_code[j]) + ', o: ' + NoShare_old[j] + ', n: ' + NoShare[j]);
            NoShare_old[j] = NoShare[j];
        	}
    	}        
    }    
    if (today >= FinalDate){ // 마지막날       
        logger.info('list_index: ' + list_index);
        logger.info('NumOfStocks: ' +  NumOfStocks);
        logger.info('load_failure_list: [' + load_failure_list + ']');
        logger.info('DelistingDate_Error_list: [' + DelistingDate_Error_list + ']');        
	}   
}
