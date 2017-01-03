/**
 * Created by zackpeng on 11/11/16.
 */

public class E_Driver {
    public static void main(String[] args) throws Exception {

        A_DataDividerByUser dataDividerByUser = new A_DataDividerByUser();
        B_CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new B_CoOccurrenceMatrixGenerator();
        C_Multiplication multiplication = new C_Multiplication();
        D_RecommenderListGenerator generator = new D_RecommenderListGenerator();

        String[] path1 = {args[0], args[1]};
        String[] path2 = {args[1], args[2]};
        String[] path3 = {args[4], args[0], args[3]};
        String[] path4 = {args[5], args[6], args[3], args[7]};

        dataDividerByUser.main(path1);
        coOccurrenceMatrixGenerator.main(path2);
        multiplication.main(path3);
        generator.main(path4);
    }

}
