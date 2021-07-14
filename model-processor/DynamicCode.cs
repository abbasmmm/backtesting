using System;

namespace Model
{
    public class Code
    {
        public static double callPrice(
double s,
double x,
double r,
double sigma,
double t)
        {
            var a = (Math.Log(s / x) + (r + sigma * sigma / 2.0) * t) / (sigma * Math.Sqrt(t));
            var b = a - sigma * Math.Sqrt(t);
            return s * cdf(a) - x * Math.Exp(-r * t) * cdf(b);
        }


        public static double phi(double x)
        {
            return Math.Exp(-x * x / 2.0) / Math.Sqrt(2.0 * Math.PI);
        }

        public static double pdf(double x, double mu = 0.0, double sigma = 1.0)
        {
            return phi((x - mu) / sigma) / sigma;
        }

        public static double Phi(double z)
        {
            if (z < -8.0)
            {
                return 0.0;
            }
            if (z > 8.0)
            {
                return 1.0;
            }
            var total = 0.0;
            var term = z;
            var i = 3;
            while (total != total + term)
            {
                total += term;
                term *= z * z / i;
                i += 2;
            }
            return 0.5 + total * phi(z);
        }

        public static double cdf(double z, double mu = 0.0, double sigma = 1.0)
        {
            return Phi((z - mu) / sigma);
        }


    }
}