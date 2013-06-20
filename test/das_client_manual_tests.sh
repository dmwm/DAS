#SEMI-MANUAL TESTS (require instalation with https proxy):

DAS_CLIENT=das_client.py

echo '\n\n\n-----------------------------' 
echo '\n\n\nexpected: multiple wildcard interpretations (Q: dataset dataset=*Zmm*)'
echo '-----------------------------'
python $DAS_CLIENT --query "dataset dataset=*Zmm*"  --host https://localhost  
 
echo '\n\n\n-----------------------------' 
echo 'expected: no given dataset exists (Q: dataset dataset=*Zmmsjhdjshfjshjf*)'
echo '-----------------------------'
python $DAS_CLIENT --query "dataset dataset=*Zmmsjhdjshfjshjf*"  --host https://localhost  
  
echo '\n\n\n-----------------------------'
echo 'expected: 1 immediate result (Q: dataset dataset=/Zmm/*/*)'
echo '-----------------------------'
python $DAS_CLIENT --query "dataset dataset=/Zmm/*/*"  --host https://localhost  

echo '\n\n\n-----------------------------'
echo 'expected: DASPLY error - non valid DAS query. Q: dataset dataxset=*Zmmsjhdjshfjshjf*'
echo '-----------------------------'
python $DAS_CLIENT --query "dataset dataxset=*Zmmsjhdjshfjshjf*"  --host https://localhost
