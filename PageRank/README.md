# PageRank

* web page들의 우선순위를 구하는 방법
* 검색 엔진에서 검색 결과를 보여주는 순서로 활용
* 중요도가 높은 page가 link한 페이지 역시 중요도가 높을 것이다
* PR(A) = (1-d) / |D| + d(PR(T1) / C(T1) + … + PR(Tn) / C(Tn) -> 웹 페이지 A의 PageRank는 A를 링크하는 페이지들의 기여의 합)
  * d : damping factor
  * |D| : #of web pages
  * PR(X) : web page X의 pagerank
  * C(X) : web page X가 가지고 있는 링크의 수